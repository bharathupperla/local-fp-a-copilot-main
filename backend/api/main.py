from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import os
import json
import httpx
import time
from typing import Dict, List

from engine.schema_loader import generate_schema
from llm.local_llm import ask_llm
from engine.intent_parser import parse_intent, set_known_companies, set_known_workers
from engine.validator import validate_intent
from engine.executor import execute, clear_cache, clear_customer_cache, normalize_filter_value
from engine.column_resolver import build_column_map, resolve_dataframe_column
from engine.metadata_resolver import load_metadata
from api.file_processor import process_uploaded_file

app = FastAPI(title="Local FP&A Chatbot")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://localhost:8080",
        "http://127.0.0.1:5173",
        "http://127.0.0.1:8080",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DATA_PATH     = "data/finance.parquet"
SCHEMA_PATH   = "schema/schema.json"
METADATA_PATH = "schema/metadata.json"
OLLAMA_URL    = "http://localhost:11434"

if not os.path.exists(DATA_PATH):
    raise FileNotFoundError(f"Parquet file not found at {DATA_PATH}")
if not os.path.exists(METADATA_PATH):
    raise FileNotFoundError("metadata.json not found. Run metadata_generator.py first.")

df     = pd.read_parquet(DATA_PATH)
schema = generate_schema(df, SCHEMA_PATH)

# Give intent parser the real company list so it can match names at start of questions
if "Customer Name" in df.columns:
    set_known_companies(df["Customer Name"].dropna().unique().tolist())
if "Worker Name" in df.columns:
    set_known_workers(df["Worker Name"].dropna().unique().tolist())


# -------------------------------------------------
# PRE-WARM OLLAMA  — loads model into memory on startup
# so the first real request doesn't cold-start
# -------------------------------------------------
def warmup_ollama():
    try:
        print("[WARMUP] Loading llama3.2:1b into memory...")
        resp = httpx.post(
            f"{OLLAMA_URL}/api/generate",
            json={"model": "llama3.2:1b", "prompt": "hi", "stream": False,
                  "options": {"num_predict": 1}},
            timeout=60.0
        )
        if resp.status_code == 200:
            print("[WARMUP] Model ready.")
        else:
            print(f"[WARMUP] Status {resp.status_code}")
    except Exception as e:
        print(f"[WARMUP FAILED] {e} — model will cold-start on first request")

warmup_ollama()


# -------------------------------------------------
# CONVERSATION & CACHE STATE
# -------------------------------------------------
_conversations:    Dict[str, List[Dict[str, str]]] = {}
_fact_sheet_cache: Dict[str, str]   = {}
_fact_sheet_ts:    Dict[str, float] = {}
_session_company:  Dict[str, str]   = {}
_session_worker:   Dict[str, str]   = {}   # tracks last explicitly queried worker
CACHE_TTL = 600

def get_history(sid):
    return _conversations.get(sid, [])

def append_history(sid, role, content):
    _conversations.setdefault(sid, []).append({"role": role, "content": content})
    _conversations[sid] = _conversations[sid][-20:]

def get_fact_sheet(company):
    k = company.lower().strip()
    if k in _fact_sheet_cache and time.time() - _fact_sheet_ts.get(k, 0) < CACHE_TTL:
        return _fact_sheet_cache[k]
    return None

def set_fact_sheet(company, facts):
    k = company.lower().strip()
    _fact_sheet_cache[k] = facts
    _fact_sheet_ts[k] = time.time()


# -------------------------------------------------
# HOT RELOAD
# -------------------------------------------------
def reload_data():
    global df, schema
    df     = pd.read_parquet(DATA_PATH)
    schema = generate_schema(df, SCHEMA_PATH)
    clear_cache()
    global _fact_sheet_cache, _fact_sheet_ts, _conversations, _session_company, _session_worker
    _fact_sheet_cache = {}
    _fact_sheet_ts    = {}
    _conversations    = {}
    _session_company  = {}
    _session_worker   = {}
    if "Customer Name" in df.columns:
        set_known_companies(df["Customer Name"].dropna().unique().tolist())
    if "Worker Name" in df.columns:
        set_known_workers(df["Worker Name"].dropna().unique().tolist())
    print(f"[HOT RELOAD] Now serving: {len(df)} rows × {len(df.columns)} columns")


# -------------------------------------------------
# FORMATTERS
# -------------------------------------------------
def _M(v):
    """Currency with full precision — no rounding, no abbreviation."""
    if v is None: return "N/A"
    v = float(v)
    return f"${v:,.2f}"

def _P(v): return "N/A" if v is None else f"{float(v):.1f}%"
def _N(v, d=0): return "N/A" if v is None else f"{float(v):,.{d}f}"


# -------------------------------------------------
# CLEAN DISPLAY SUMMARY
# Shows exactly: Revenue, GM$, GM%, MSP, Active Headcount, CS/CD, Group/Sector
# -------------------------------------------------
def build_display_summary(p: dict) -> str:
    fin   = p.get("financial_summary", {})
    w     = p.get("worker_analysis", {})
    cats  = p.get("categorical_data", {})
    keys  = p.get("key_personnel", {})
    recs  = p.get("recruiter_analysis", {})
    cycles = p.get("cycle_breakdown", [])

    # ── Core metrics ────────────────────────────────────────────────────
    total_rev  = _M(fin.get("revenue", {}).get("total"))
    total_gm   = _M(fin.get("gross_margin_dollars", {}).get("total"))
    gm_pct     = _P(fin.get("gross_margin_percentage", {}).get("average"))
    msp        = (cats.get("msp_list") or ["N/A"])[0]
    msp_type   = (cats.get("msp_ht_vop") or ["N/A"])[0]

    # Active headcount = total unique workers in the dataset
    active_headcount = str(w.get("total_workers", "N/A"))

    def clean(lst):
        return ", ".join(x for x in (lst or []) if x not in ("0", "nan", None, "0.0"))

    cs_cd     = clean(cats.get("cs_cd_teams")) or "N/A"
    msp_types = clean(cats.get("msp_ht_vop")) or msp_type
    vertical_clients = cats.get("vertical_clients") or []
    industry         = (cats.get("industries") or ["N/A"])[0]
    sector = ", ".join(vertical_clients) if vertical_clients else industry

    lines = []

    lines.append("**KEY METRICS**")
    lines.append(f"- **Total Revenue:** {total_rev}")
    lines.append(f"- **Total GM:** {total_gm}")
    lines.append(f"- **GM%:** {gm_pct}")
    lines.append(f"- **MSP:** {msp} ({msp_types})")
    lines.append(f"- **Active Headcount:** {active_headcount}")
    lines.append(f"- **CS / CD:** {cs_cd}")
    lines.append(f"- **Group / Sector:** {sector}")

    return "\n".join(lines)


# -------------------------------------------------
# FULL FACT SHEET  (follow-up context only — never shown to user)
# -------------------------------------------------
def build_full_fact_sheet(p: dict) -> str:
    fin    = p.get("financial_summary", {})
    hrs    = p.get("hours_analysis", {})
    rates  = p.get("rate_analysis", {})
    perf   = p.get("performance_indicators", {})
    trends = p.get("cycle_trends", {})
    locs   = p.get("location_breakdown", [])
    loc_s  = p.get("location_summary", {})
    w      = p.get("worker_analysis", {})
    cats   = p.get("categorical_data", {})
    jobs   = p.get("job_category_breakdown", [])
    recs   = p.get("recruiter_analysis", {})
    dates  = p.get("date_analysis", {})
    cycles = p.get("cycle_breakdown", [])
    qtrs   = p.get("quarter_breakdown", [])
    wt     = p.get("work_type_breakdown", [])
    keys   = p.get("key_personnel", {})

    lines = []
    def s(k, v): lines.append(f"{k}: {v}")

    def clean(lst):
        return ", ".join(x for x in (lst or []) if x not in ("0", "nan", None))

    lines.append("=== IDENTITY ===")
    s("Company",          p.get("company"))
    s("Industry",         (cats.get("industries") or ["N/A"])[0])
    s("MSP",              (cats.get("msp_list") or ["N/A"])[0])
    s("MSP Type",         (cats.get("msp_ht_vop") or ["N/A"])[0])
    s("Countries",        ", ".join(cats.get("countries") or []))
    s("Work Types",       ", ".join(x.get("work_type", "") for x in wt))
    s("Vertical Teams",   ", ".join(cats.get("vertical_teams") or []))
    s("Vertical Clients", ", ".join(cats.get("vertical_clients") or []))
    s("Records",          p.get("total_records"))
    s("Total Workers",    w.get("total_workers"))
    s("Join Range",       f"{(dates.get('join_dates',{}).get('earliest','')[:10])} to {(dates.get('join_dates',{}).get('latest','')[:10])}")

    lines.append("\n=== FINANCIALS ===")
    s("Total Revenue",  _M(fin.get("revenue",{}).get("total")))
    s("Net Revenue",    _M(fin.get("net_revenue",{}).get("total")))
    s("Base Cost",      _M(fin.get("costs",{}).get("total_base_cost")))
    s("Loaded Cost",    _M(fin.get("costs",{}).get("total_loaded_cost")))
    s("Total GM$",      _M(fin.get("gross_margin_dollars",{}).get("total")))
    s("Avg GM%",        _P(fin.get("gross_margin_percentage",{}).get("average")))
    s("GM% Range",      f"{_P(fin.get('gross_margin_percentage',{}).get('min'))} – {_P(fin.get('gross_margin_percentage',{}).get('max'))}")
    s("Avg Markup",     _P(fin.get("markup",{}).get("avg_markup_pct")))
    s("VMS Fees",       _M(fin.get("vms",{}).get("total_vms_fees")))
    s("Avg VMS%",       _P(fin.get("vms",{}).get("avg_vms_pct")))

    lines.append("\n=== HOURS & RATES ===")
    s("Total Hours",  _N(hrs.get("total_hours",{}).get("total")))
    s("Regular",      f"{_N(hrs.get('regular_hours',{}).get('total'))} ({_P(hrs.get('regular_hours',{}).get('percentage_of_total'))})")
    s("Overtime",     f"{_N(hrs.get('overtime_hours',{}).get('total'))} ({_P(hrs.get('overtime_hours',{}).get('percentage_of_total'))})")
    s("Double Time",  f"{_N(hrs.get('double_time_hours',{}).get('total'))} ({_P(hrs.get('double_time_hours',{}).get('percentage_of_total'))})")
    s("Bill Rate",    f"{_M(rates.get('bill_rates',{}).get('regular',{}).get('average'))} (range {_M(rates.get('bill_rates',{}).get('regular',{}).get('min'))}–{_M(rates.get('bill_rates',{}).get('regular',{}).get('max'))})")
    s("Pay Rate",     f"{_M(rates.get('pay_rates',{}).get('regular',{}).get('average'))} (range {_M(rates.get('pay_rates',{}).get('regular',{}).get('min'))}–{_M(rates.get('pay_rates',{}).get('regular',{}).get('max'))})")
    s("Spread",       _M(rates.get("spread",{}).get("average")))
    s("GM/Hour",      _M(hrs.get("gm_per_hour",{}).get("average")))
    s("Revenue/Hour", _M(perf.get("revenue_per_hour")))
    s("Profit/Hour",  _M(perf.get("profit_per_hour")))

    lines.append("\n=== CYCLES ===")
    for c in cycles:
        lines.append(f"  {c['cycle']}: Rev={_M(c['revenue_total'])}, GM$={_M(c['gm_dollars_total'])}, GM%={_P(c['gm_pct_avg'])}, Hours={_N(c['total_hours'])}, Workers={c['worker_count']}")
    s("Best Cycle",     f"{trends.get('best_revenue_cycle')} ({_M(trends.get('best_revenue_amount'))})")
    s("Worst Cycle",    f"{trends.get('worst_revenue_cycle')} ({_M(trends.get('worst_revenue_amount'))})")
    s("Revenue Growth", _P(trends.get("revenue_growth")))

    lines.append("\n=== QUARTERS ===")
    for q in qtrs:
        lines.append(f"  {q['quarter']}: Rev={_M(q['revenue_total'])}, GM$={_M(q['gm_dollars_total'])}, GM%={_P(q['gm_pct_avg'])}, Hours={_N(q['total_hours'])}, Workers={q['worker_count']}")

    lines.append("\n=== LOCATIONS ===")
    s("Total", loc_s.get("total_locations"))
    for l in locs:
        lines.append(f"  {l['location']}: Rev={_M(l['revenue'])}, GM$={_M(l['gm_dollars'])}, GM%={_P(l['gm_pct_avg'])}, Workers={l['worker_count']}")

    lines.append("\n=== JOB CATEGORIES ===")
    for j in jobs:
        lines.append(f"  {j['category']}: Rev={_M(j['revenue'])}, GM$={_M(j['gm_dollars'])}, GM%={_P(j['gm_pct_avg'])}, Workers={j['worker_count']}")

    lines.append("\n=== WORKERS ===")
    s("Total",          w.get("total_workers"))
    s("Avg Rev/Worker", _M(w.get("avg_revenue_per_worker")))
    s("Avg Hrs/Worker", _N(w.get("avg_hours_per_worker"), 1))
    lines.append("Top by Revenue:")
    for wk in w.get("top_workers_by_revenue", [])[:10]:
        lines.append(f"  {wk['worker_name']}: Rev={_M(wk['revenue'])}, GM%={_P(wk['gm_pct_avg'])}, Hours={_N(wk['total_hours'])}")
    lines.append("Top by Margin:")
    for wk in w.get("top_workers_by_margin", [])[:5]:
        lines.append(f"  {wk['worker_name']}: GM%={_P(wk['gm_pct_avg'])}, Rev={_M(wk['revenue'])}")
    lines.append("Bottom by Revenue:")
    for wk in w.get("bottom_workers_by_revenue", []):
        lines.append(f"  {wk['worker_name']}: Rev={_M(wk['revenue'])}, GM%={_P(wk['gm_pct_avg'])}")

    lines.append("\n=== RECRUITERS ===")
    s("Total", recs.get("total_recruiters"))
    for r in recs.get("top_recruiters", [])[:10]:
        lines.append(f"  {r['recruiter']}: Rev={_M(r['revenue'])}, GM$={_M(r['gm_dollars'])}, Workers={r['worker_count']}")

    lines.append("\n=== KEY PERSONNEL ===")
    s("CDL",          clean(keys.get("cdl_list")))
    s("CDM",          clean(keys.get("cdm_list")))
    s("Sourcing CSA", clean(keys.get("sourcing_csa")))
    s("Sourcing CSM", clean(keys.get("sourcing_csm")))

    return "\n".join(lines)


# -------------------------------------------------
# PREAMBLE STRIPPER
# -------------------------------------------------
PREAMBLE_PATTERNS = [
    "here are the two paragraphs",
    "here are two paragraphs",
    "here is the paragraph",
    "here is a short paragraph",
    "here is a paragraph",
    "here's a short paragraph",
    "here's a paragraph",
    "as an fp&a analyst",
    "as a fp&a analyst",
    "based on the provided facts",
    "based on the facts",
    "paragraph 1",
    "paragraph 2",
    "intro:",
    "closing:",
    "**paragraph",
    "note:",
    "sure,",
    "certainly,",
    "of course,",
]

def _strip_preamble(text: str) -> str:
    lines = text.strip().split("\n")
    cleaned = []
    for line in lines:
        low = line.strip().lower()
        if any(low.startswith(pat) for pat in PREAMBLE_PATTERNS):
            continue
        cleaned.append(line)
    return "\n".join(cleaned).strip()


# -------------------------------------------------
# INTRO + CLOSING
# Python builds the sentences — LLM only rephrases the closing
# No hallucination possible — all values come from Python
# -------------------------------------------------
def llm_intro_and_closing(p: dict, model: str | None = None) -> tuple[str, str]:
    company  = p.get("company", "")
    cats     = p.get("categorical_data", {})
    fin      = p.get("financial_summary", {})
    w        = p.get("worker_analysis", {})
    trends   = p.get("cycle_trends", {})
    locs     = p.get("location_summary", {})
    jobs     = p.get("job_category_breakdown", [])
    recs     = p.get("recruiter_analysis", {}).get("top_recruiters", [])

    industry = (cats.get("industries") or ["N/A"])[0]
    msp      = (cats.get("msp_list") or ["N/A"])[0]

    def clean(lst):
        return ", ".join(x for x in (lst or []) if x not in ("0", "nan", None, "0.0"))

    msp_types        = clean(cats.get("msp_ht_vop")) or "N/A"
    countries        = ", ".join(cats.get("countries") or ["N/A"])
    cs_cd            = clean(cats.get("cs_cd_teams")) or "N/A"
    vertical_clients = cats.get("vertical_clients") or []
    sector           = ", ".join(vertical_clients) if vertical_clients else industry
    active_hc        = str(w.get("total_workers", "N/A"))
    rev              = _M(fin.get("revenue", {}).get("total"))
    total_gm         = _M(fin.get("gross_margin_dollars", {}).get("total"))
    gm_pct           = _P(fin.get("gross_margin_percentage", {}).get("average"))

    # Closing facts — pulled directly from data, no LLM interpretation
    best_c   = trends.get("best_revenue_cycle", "N/A")
    worst_c  = trends.get("worst_revenue_cycle", "N/A")
    growth   = _P(trends.get("revenue_growth")) if trends.get("revenue_growth") is not None else "N/A"
    top_loc  = locs.get("top_location_by_revenue", "N/A")
    top_job  = jobs[0].get("category", "N/A") if jobs else "N/A"
    top_rec  = recs[0].get("recruiter", "N/A") if recs else "N/A"

    # Python intro — accurate, no LLM
    intro = (
        f"{company} is a {industry} industry account managed through "
        f"{msp} ({msp_types}), operating in the {sector} sector "
        f"across {countries} with {active_hc} workers on record."
    )

    # LLM only rewrites intro into natural sentences — never writes the closing
    try:
        key_facts = (
            f"Company: {company}, Industry: {industry}, MSP: {msp} ({msp_types}), "
            f"Countries: {countries}, Sector: {sector}, Workers: {active_hc}, "
            f"Revenue: {rev}, GM: {total_gm}, GM%: {gm_pct}, CS/CD: {cs_cd}"
        )
        prompt = f"""Write 1 short paragraph (2-3 sentences) as an FP&A analyst.
Use ONLY these facts. Do not add anything not listed. No invented details.
Facts: {key_facts}
Cover: company name, industry, MSP, sector, countries, worker count.
Begin:"""

        collected = []
        import httpx as _httpx
        with _httpx.Client(timeout=_httpx.Timeout(30.0)) as client:
            with client.stream("POST", f"{OLLAMA_URL}/api/generate", json={
                "model": model or "llama3.2:1b",
                "prompt": prompt,
                "stream": True,
                "options": {"temperature": 0.2, "num_ctx": 256, "num_predict": 100}
            }) as resp:
                resp.raise_for_status()
                for line in resp.iter_lines():
                    if line:
                        try:
                            import json as _json
                            chunk = _json.loads(line)
                            collected.append(chunk.get("response", ""))
                            if chunk.get("done"): break
                        except: continue

        llm_intro = _strip_preamble("".join(collected)).strip()
        if llm_intro and len(llm_intro) > 20:
            intro = llm_intro

    except Exception as e:
        print(f"[LLM INTRO FAILED] {e}")

    # Closing is always Python — never LLM — to prevent hallucination
    closing_parts = []
    closing_parts.append(f"The account generated {rev} in total revenue with a gross margin of {total_gm} at {gm_pct}.")

    if best_c != "N/A" and worst_c != "N/A":
        closing_parts.append(f"{best_c} was the strongest cycle and {worst_c} the weakest, with revenue growth of {growth}.")
    elif best_c != "N/A":
        closing_parts.append(f"{best_c} was the strongest billing cycle.")

    if top_loc != "N/A":
        closing_parts.append(f"{top_loc} leads by location revenue.")
    if top_job != "N/A":
        closing_parts.append(f"{top_job} is the top job category.")
    if top_rec != "N/A" and top_rec not in ("0", "nan"):
        closing_parts.append(f"{top_rec} is the top recruiter.")

    closing = " ".join(closing_parts)
    return intro, closing


# -------------------------------------------------
# SLIM FACT SHEET  (for follow-up LLM calls)
# -------------------------------------------------
def _slim_fact_sheet(fact_sheet: str) -> str:
    keep_sections = [
        "=== IDENTITY ===",
        "=== FINANCIALS ===",
        "=== HOURS & RATES ===",
        "=== CYCLES ===",
        "=== QUARTERS ===",
        "=== LOCATIONS ===",
        "=== JOB CATEGORIES ===",
        "=== KEY PERSONNEL ===",
    ]
    lines = fact_sheet.split("\n")
    result = []
    current_ok = True

    for line in lines:
        if line.strip().startswith("==="):
            current_ok = any(s in line for s in keep_sections)
        if current_ok:
            stripped = line.strip()
            if stripped in ("Top by Revenue:", "Top by Margin:", "Bottom by Revenue:"):
                continue
            if stripped.startswith("  ") and "Rev=" in stripped and "GM%=" in stripped and "Hours=" in stripped:
                continue
            result.append(line)

    return "\n".join(result)


# -------------------------------------------------
# LLM FOLLOW-UP
# -------------------------------------------------
def _route_question(question: str, fact_sheet: str) -> str:
    """
    Detects what the question is about and returns ONLY the relevant
    section of the fact sheet. Cuts context from 3000 tokens to ~300.
    """
    q = question.lower()

    # Map keywords to fact sheet section markers
    section_map = [
        # Cycles — match any digit after cycle/c
        (["cycle", "cyc", "cycl", "billing period", "billing cycle",
          "c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9",
          "cycle 0", "cycle 1", "cycle 2", "cycle 3", "cycle 4",
          "cycle 5", "cycle 6", "cycle 7", "cycle 8", "cycle 9",
          "cycle 10", "cycle 11", "cycle 12",
          "best cycle", "worst cycle", "growth", "billing"],
         "=== CYCLES ==="),
        # Quarters
        (["quarter", "qtr", "q1", "q2", "q3", "q4", "quarterly"],
         "=== QUARTERS ==="),
        # Workers / associates / headcount
        (["worker", "associate", "employee", "staff", "people", "person",
          "headcount", "hc", "top worker", "bottom worker", "highest rev",
          "who earn", "who made", "who has", "who is the top", "who are"],
         "=== WORKERS ==="),
        # Recruiters
        (["recruiter", "recruiting", "recruit", "sourcer", "sourcing"],
         "=== RECRUITERS ==="),
        # Locations
        (["location", "loc", "state", "city", "region", "where", "mn", "il",
          "ca", "tx", "pr", "pa", "nc", "nj", "ny", "fl", "ma", "co", "in", "oh", "va"],
         "=== LOCATIONS ==="),
        # Job categories
        (["job", "category", "categor", "role", "it ", "engineering", "scientific",
          "administrative", "clinical", "financial", "purchasing", "quality"],
         "=== JOB CATEGORIES ==="),
        # Hours and rates
        (["hour", "hrs", "bill rate", "pay rate", "br", "br/hr", "spread",
          "overtime", "ot", "double time", "dt", "regular hours", "gm/hour",
          "revenue/hour", "profit/hour", "markup", "load factor"],
         "=== HOURS & RATES ==="),
        # Financials
        (["revenue", "rev", "revenu", "gross margin", "gm$", "gm%", "gm ",
          "cost", "vms", "net revenue", "markup", "margin", "profit",
          "financial", "finance", "money", "total", "earning"],
         "=== FINANCIALS ==="),
        # Key personnel / CS/CD
        (["cdl", "cdm", "csa", "csm", "cs/cd", "personnel", "key person",
          "delivery lead", "delivery manager", "sourcing"],
         "=== KEY PERSONNEL ==="),
        # Identity / account info
        (["msp", "industry", "country", "countries", "work type", "vertical",
          "sector", "group", "account", "platform", "tapfin", "allegis"],
         "=== IDENTITY ==="),
    ]

    # Collect matching sections
    matched_sections = []
    lines = fact_sheet.split("\n")
    current_section = None
    sections: dict = {}

    for line in lines:
        if line.strip().startswith("==="):
            current_section = line.strip()
            sections[current_section] = []
        elif current_section:
            sections[current_section].append(line)

    for keywords, section_key in section_map:
        if any(kw in q for kw in keywords):
            # Find the matching section key (partial match)
            for sk in sections:
                if section_key.replace("===", "").strip() in sk:
                    matched_sections.append(section_key.replace("===", "").strip())
                    break

    if not matched_sections:
        # Fallback — send identity + financials only
        matched_sections = ["IDENTITY", "FINANCIALS"]

    # Always include IDENTITY for context
    if "IDENTITY" not in matched_sections:
        matched_sections.insert(0, "IDENTITY")

    # Build the slim context from matched sections only
    result_lines = []
    current_section = None
    include = False

    for line in lines:
        if line.strip().startswith("==="):
            section_name = line.strip().replace("===", "").strip()
            include = any(s in section_name for s in matched_sections)
            current_section = section_name
            if include:
                result_lines.append(line)
        elif include:
            result_lines.append(line)

    return "\n".join(result_lines)


def call_llm_followup(question: str, fact_sheet: str, history: List[Dict[str, str]], model: str | None = None) -> str:
    # Route question to relevant section only — keeps context tiny = fast
    routed_context = _route_question(question, fact_sheet)

    recent = history[-6:]
    conversation = "\n".join(
        f"{'User' if t['role'] == 'user' else 'Assistant'}: {t['content']}"
        for t in recent
    )

    normalized_q = _normalize_question(question)

    prompt = f"""You are an FP&A analyst working with internal workforce billing data.
Worker names in the FACT SHEET are employees/contractors — always answer questions about them using the data.
Never refuse to answer about workers — this is internal business data, not public information.
Answer using ONLY the data below. Be direct and concise.

UNDERSTAND ABBREVIATIONS & VARIATIONS:
- rev / revenu / reveue / reve / total rev = revenue
- gm / gm$ / gm% / gross margin / margin = gross margin ($ or %)
- br / br/hr / bill rate / billing rate / avg br = bill rate
- pr / pay / pay rate / payrate = pay rate
- hc / headcount / associates / workers / employees / people / staff / resources = worker count
- ot = overtime  |  dt = double time  |  avg = average  |  spread = bill rate minus pay rate
- Cycle 01–12 = specific billing cycle (already normalized from any input format)
- Q1–Q4 = specific quarter (already normalized)
- Week 01+ = specific week

CRITICAL FILTER RULE:
If the question specifies a Cycle, Quarter, Week, Location, Worker, or Recruiter —
find ONLY that specific entry in the data and return its exact numbers.
NEVER return overall totals when a specific filter is given.
Example: "revenue in Cycle 02" → return ONLY Cycle 02 revenue, not total revenue.

DATA:
{routed_context}

HISTORY:
{conversation if conversation.strip() else "(none)"}

Q: {normalized_q}
A:"""

    collected = []
    try:
        with httpx.Client(timeout=httpx.Timeout(60.0)) as client:
            with client.stream("POST", f"{OLLAMA_URL}/api/generate", json={
                "model": model or "llama3.2:1b",
                "prompt": prompt,
                "stream": True,
                "options": {"temperature": 0.1, "num_ctx": 1024, "num_predict": 250}
            }) as resp:
                resp.raise_for_status()
                for line in resp.iter_lines():
                    if line:
                        try:
                            chunk = json.loads(line)
                            collected.append(chunk.get("response", ""))
                            if chunk.get("done"): break
                        except: continue
        return "".join(collected).strip()
    except Exception as e:
        print(f"[LLM FOLLOWUP FAILED] {e}")
        return ""


# -------------------------------------------------
# PROFILE HANDLER
# -------------------------------------------------
def handle_company_profile(profile: dict, question: str, session_id: str, model: str | None = None) -> str:
    company = profile.get("company", "")

    fact_sheet = get_fact_sheet(company)
    if not fact_sheet:
        fact_sheet = build_full_fact_sheet(profile)
        set_fact_sheet(company, fact_sheet)

    _session_company[session_id] = company

    display        = build_display_summary(profile)
    intro, closing = llm_intro_and_closing(profile, model)

    parts = [intro, display]
    if closing:
        parts.append(closing)
    answer = "\n\n".join(parts)

    append_history(session_id, "user", question)
    append_history(session_id, "assistant", answer)
    return answer


# -------------------------------------------------
# COMPARE HANDLER
# Handles "compare X in cycle 3 and cycle 4" type questions
# Runs two deterministic queries and returns a combined answer
# -------------------------------------------------
def _handle_compare(question: str, session_id: str, model: str | None = None) -> str | None:
    """
    Detects compare/vs questions with two filters and runs both deterministic queries.
    Returns a combined answer string or None if not a compare question.
    """
    import re as _re
    q_lower = question.lower()

    # Must be a compare question
    if not any(w in q_lower for w in ["compare", "vs", "versus", "difference between", "and cycle", "and q", "and week", "and quarter"]):
        return None

    # Detect two cycles: "cycle 3 and cycle 4" or "Cycle 03 and Cycle 04"
    cycles = _re.findall(r'cycle\s*0?(\d{1,2})', q_lower)
    quarters = _re.findall(r'q([1-4])', q_lower)
    weeks = _re.findall(r'week\s*0?(\d{1,2})', q_lower)

    # Need exactly 2 of the same type
    filters_list = []
    filter_key = None
    display_key = None

    if len(cycles) >= 2:
        filter_key  = "cycle"
        display_key = "Cycle"
        filters_list = [f"Cycle {int(c):02d}" for c in cycles[:2]]
    elif len(quarters) >= 2:
        filter_key  = "quarter"
        display_key = "Quarter"
        filters_list = [f"Q{q}" for q in quarters[:2]]
    elif len(weeks) >= 2:
        filter_key  = "week_num"
        display_key = "Week"
        filters_list = [f"Week {int(w):02d}" for w in weeks[:2]]
    else:
        return None

    # Infer measure from question
    from engine.intent_parser import _infer_measure_from_question
    measure = _infer_measure_from_question(question) or "revenue"

    # Get company from question or session
    company = _session_company.get(session_id)
    cust_match = _re.search(r'(?:of|for)\s+([a-z0-9\s&\-\.\,\(\)\/]+?)(?:\s+in|\s+vs|\s+and\s+(?:cycle|q\d|quarter|week)|\?|$)', q_lower)
    if cust_match:
        candidate = cust_match.group(1).strip().rstrip(".,?")
        skip = {"cycle", "quarter", "week", "q1","q2","q3","q4","the","a","an"}
        if candidate.lower() not in skip and len(candidate) > 1:
            # Resolve against actual company names
            matches = [c for c in df["Customer Name"].dropna().unique()
                      if candidate.lower() in c.lower() or c.lower() in candidate.lower()]
            if matches:
                company = matches[0]

    if not company:
        return None

    # Run two deterministic queries
    results = []
    for fval in filters_list:
        intent = {
            "measures": [measure],
            "filters": {"customer_name": company, filter_key: fval},
            "aggregation": "avg" if measure in ("bill_rate_reg", "pay_rate_reg", "gm_pct") else "sum"
        }
        try:
            from engine.validator import validate_intent
            if "headcount" not in intent.get("measures", []):
                validate_intent(intent, schema, question)
            raw = execute(intent, df, schema)
            formatted = _format_deterministic(raw, intent, f"{measure} of {company} in {fval}")
            results.append((fval, raw, formatted))
        except Exception as e:
            print(f"[COMPARE MISS] {fval}: {e}")

    if len(results) < 2:
        return None

    # Build comparison answer
    label = {
        "revenue": "Total Revenue", "gm_dollars": "Gross Margin ($)",
        "gm_pct": "Gross Margin %", "bill_rate_reg": "Avg Bill Rate",
        "pay_rate_reg": "Avg Pay Rate", "total_hours": "Total Hours",
        "base_cost": "Base Cost",
    }.get(measure, measure.replace("_", " ").title())

    v1, v2 = results[0][1], results[1][1]
    f1, f2 = results[0][0], results[1][0]
    fmt1   = f"${v1:,.2f}" if measure not in ("gm_pct",) else f"{v1:.1f}%"
    fmt2   = f"${v2:,.2f}" if measure not in ("gm_pct",) else f"{v2:.1f}%"

    diff     = abs(v1 - v2)
    diff_fmt = f"${diff:,.2f}" if measure not in ("gm_pct",) else f"{diff:.1f}%"
    higher   = f1 if v1 > v2 else f2
    pct_diff = round(abs(v1 - v2) / max(min(v1, v2), 0.01) * 100, 1)

    answer = (
        f"**{label} Comparison for {company}**\n\n"

        f"- **{f1}:** {fmt1}\n"

        f"- **{f2}:** {fmt2}\n\n"

        f"**{higher}** was higher by {diff_fmt} ({pct_diff}%)."
    )
    return answer


# -------------------------------------------------
# COMPARE QUESTION HANDLER
# Handles: "compare revenue of AbbVie in cycle 3 and cycle 4"
# -------------------------------------------------
def _handle_compare_question(normalized: str, original: str, intent: dict, session_id: str, model=None) -> str | None:
    """
    Detects if question compares two time periods and runs both deterministic queries.
    Returns a formatted comparison string, or None if not a compare question.
    """
    import re as _re

    q = normalized.lower()

    # Must be a compare/vs question or mention two cycles/quarters/weeks
    compare_triggers = ["compare", "vs", "versus", "difference between", "and cycle", "and quarter", "and week", "and q"]
    if not any(t in q for t in compare_triggers):
        return None

    measures = intent.get("measures", [])
    if not measures:
        return None

    # Find all cycle mentions: "Cycle 03", "Cycle 04"
    cycles   = _re.findall(r"Cycle\s*(\d{1,2})", normalized, _re.IGNORECASE)
    quarters = _re.findall(r"Q([1-4])", normalized, _re.IGNORECASE)
    weeks    = _re.findall(r"Week\s*(\d{1,2})", normalized, _re.IGNORECASE)

    periods = []
    for c in cycles:   periods.append(("cycle",   f"Cycle {int(c):02d}"))
    for q2 in quarters: periods.append(("quarter", f"Q{q2}"))
    for w in weeks:    periods.append(("week_num", f"Week {int(w):02d}"))

    if len(periods) < 2:
        return None

    # Get base filters (company etc.) — remove any existing time filter
    base_filters = {k: v for k, v in intent.get("filters", {}).items()
                    if k not in ("cycle", "quarter", "week_num")}

    # Auto-inject session company if not present
    if not base_filters.get("customer_name"):
        session_cust = _session_company.get(session_id)
        if session_cust:
            base_filters["customer_name"] = session_cust

    results = []
    metadata   = load_metadata("schema/metadata.json")
    column_map = build_column_map(df.columns)

    for filter_key, filter_val in periods:
        try:
            test_intent = {
                "measures":    measures,
                "filters":     {**base_filters, filter_key: filter_val},
                "aggregation": intent.get("aggregation", "sum"),
            }
            working_df = df.copy()
            skip = {"all", ""}
            for col, val in test_intent["filters"].items():
                if str(val).lower().strip() in skip:
                    continue
                real_col = resolve_dataframe_column(col, column_map)
                if not real_col:
                    continue
                mask = normalize_filter_value(real_col, val, working_df, metadata)
                if mask is not None and mask.any():
                    working_df = working_df[mask]

            measure    = measures[0]
            real_meas  = resolve_dataframe_column(measure, column_map)
            if not real_meas:
                continue
            series = pd.to_numeric(working_df[real_meas], errors="coerce").dropna()
            if series.empty:
                continue

            agg = intent.get("aggregation", "sum")
            val = float(series.sum()) if agg == "sum" else float(series.mean())
            results.append((filter_val, val))

        except Exception as e:
            print(f"[COMPARE ERROR] {filter_val}: {e}")

    if len(results) < 2:
        return None

    # Format comparison
    company   = base_filters.get("customer_name", "")
    measure   = measures[0]
    LABELS    = {
        "revenue": "Total Revenue", "gm_dollars": "Gross Margin ($)",
        "gm_pct": "GM%", "total_hours": "Total Hours",
        "bill_rate_reg": "Avg Bill Rate", "pay_rate_reg": "Avg Pay Rate",
    }
    label = LABELS.get(measure, measure.replace("_", " ").title())

    suffix = (" for " + company) if company else ""
    lines  = [f"**{label} Comparison{suffix}:**"]
    vals  = []
    for period, val in results:
        formatted = f"${val:,.2f}" if measure not in ("gm_pct",) else f"{val:.2f}%"
        lines.append(f"- **{period}:** {formatted}")
        vals.append((period, val))

    # Add difference
    if len(vals) == 2:
        diff     = vals[1][1] - vals[0][1]
        diff_pct = (diff / vals[0][1] * 100) if vals[0][1] != 0 else 0
        sign     = "+" if diff >= 0 else ""
        formatted_diff = f"${abs(diff):,.2f}" if measure not in ("gm_pct",) else f"{abs(diff):.2f}%"
        diff_str = ("+" if diff >= 0 else "-") + "$" + f"{abs(diff):,.2f}"
        pct_str  = ("+" if diff_pct >= 0 else "") + f"{diff_pct:.1f}%"
        lines.append(f"**Difference:** {diff_str} ({pct_str})")
        higher = vals[0][0] if vals[0][1] > vals[1][1] else vals[1][0]
        lines.append(f"**{higher}** was higher.")
    return "\n".join(lines)



# -------------------------------------------------
# FILTER RESOLVER
# Fixes misclassified filters before execution:
# 1. If customer_name matches a worker — move to worker_name
# 2. If quarter value is bare "Q2" but data has "Q2 2024" — keep as-is,
#    executor handles startswith matching
# -------------------------------------------------
def _resolve_filters(intent: dict) -> None:
    """
    Fixes misclassified filters before execution.
    1. If worker_name is actually a company → move to customer_name
    2. If customer_name is actually a worker → move to worker_name
    """
    import re as _re
    filters = intent.get("filters", {})
    if not filters:
        return

    # ── Check 1: worker_name that is actually a company ──────────────────
    worker = filters.get("worker_name", "")
    if worker and "Customer Name" in df.columns:
        customers_lower = df["Customer Name"].dropna().str.lower().str.strip()
        worker_lower = worker.lower().strip()
        if (customers_lower.eq(worker_lower).any()
                or customers_lower.str.contains(_re.escape(worker_lower), regex=True, na=False).any()
                or any(worker_lower in c for c in customers_lower if len(c) > 3)):
            print(f"[FILTER RESOLVE] worker_name '{worker}' is a company → customer_name")
            filters["customer_name"] = worker
            del filters["worker_name"]

    cust = filters.get("customer_name", "")
    if not cust:
        return

    cust_lower = cust.lower().strip()

    # Step 1: Check if it matches a known customer — if yes, leave it as company
    if "Customer Name" in df.columns:
        customers_lower = df["Customer Name"].dropna().str.lower().str.strip()
        # Exact match
        if customers_lower.eq(cust_lower).any():
            return
        # Startswith match
        if customers_lower.str.startswith(cust_lower).any():
            return
        # Contains match — handles "ags - compucom", "ac culinary group llc" etc.
        if customers_lower.str.contains(_re.escape(cust_lower), regex=True, na=False).any():
            return
        # Reverse: cust_lower contains a customer name (partial typed name)
        if any(c in cust_lower for c in customers_lower if len(c) > 5):
            return

    # Step 2: Only reclassify as worker if exact or startswith match in worker list
    if "Worker Name" in df.columns:
        workers_lower = df["Worker Name"].dropna().str.lower().str.strip()

        # Exact match
        if workers_lower.eq(cust_lower).any():
            print(f"[FILTER RESOLVE] Exact worker match: '{cust}' → worker_name")
            filters["worker_name"] = cust
            del filters["customer_name"]
            return

        # Worker name starts with typed name — "alexis cruz" → "Alexis Cruz Hernandez"
        starts_match = workers_lower[workers_lower.str.startswith(cust_lower)]
        if not starts_match.empty:
            actual = df.loc[starts_match.index[0], "Worker Name"]
            print(f"[FILTER RESOLVE] Worker startswith match: '{cust}' → '{actual}'")
            filters["worker_name"] = actual
            del filters["customer_name"]
            return




# -------------------------------------------------
# TOTAL QUERY DETECTOR
# Detects when user wants whole-file totals, not a filtered subset
# -------------------------------------------------
_TOTAL_KEYWORDS = [
    "total overall", "overall total", "grand total", "all together",
    "across all", "entire file", "whole file", "all companies",
    "all customers", "all clients", "complete total", "combined total",
    "everything", "all data", "full total", "aggregate",
]
_TOTAL_PHRASES = [
    "what is total", "what is the total", "whats the total",
    "total revenue", "total gm", "total gross margin",
    "total headcount", "total hours", "total workers",
    "how many total", "overall revenue", "overall gm",
]
# Terms meaning "our company / all clients combined" — never a client filter
_OWN_COMPANY_TERMS = {"spectraforce"}

def _is_total_query(question: str) -> bool:
    """Returns True if the question is asking for whole-file totals with no company context."""
    q = question.lower().strip()
    # "spectraforce" = our own company = overall totals across all clients
    if any(term in q for term in _OWN_COMPANY_TERMS):
        return True
    # If any known client company is mentioned — it's NOT a total query
    if "Customer Name" in df.columns:
        known = df["Customer Name"].dropna().str.lower().str.strip().unique()
        if any(c in q for c in known if len(c) > 3):
            return False
    # Contains total keywords without a company name → whole file
    if any(kw in q for kw in _TOTAL_KEYWORDS):
        return True
    # Phrases can appear anywhere in the sentence (not just at start)
    if any(p in q for p in _TOTAL_PHRASES):
        return True
    return False


# -------------------------------------------------
# MSP / CLIENT LIST KEYWORD SETS  (module-level so they're compiled once)
# Catches any phrasing a user might use to ask for a list
# -------------------------------------------------
_LIST_MSP_KEYWORDS = {
    # direct "msp" variants
    "msp", "msps", "msp list", "msp lists",
    "list msp", "list msps", "list all msp", "list all msps",
    "list the msp", "list the msps",
    "show msp", "show msps", "show all msp", "show all msps",
    "show me msp", "show me the msp", "show me all msp",
    "all msp", "all msps",
    "which msp", "which msps", "what msp", "what msps",
    "what are the msp", "what are the msps",
    "what are all the msp", "what are all msps",
    "how many msp", "how many msps",
    "give me msp", "give me the msp", "give me all msp",
    "tell me msp", "tell me the msp", "tell me all msp",
    "get msp", "get all msp", "get all msps",
    "can you list msp", "can you show msp",
    "what is the msp", "what are msp",
}

_LIST_CLIENT_KEYWORDS = {
    # client/customer/account variants
    "list client", "list clients", "list all client", "list all clients",
    "list the client", "list the clients",
    "list customer", "list customers", "list all customer", "list all customers",
    "list account", "list accounts", "list all account", "list all accounts",
    "show client", "show clients", "show all client", "show all clients",
    "show me client", "show me clients", "show me all client", "show me all clients",
    "show customer", "show customers", "show all customer", "show all customers",
    "all clients", "all client", "all customers", "all customer",
    "which clients", "which client", "what clients", "what client",
    "who are the clients", "who are our clients", "who are the customers",
    "what are the clients", "what are our clients", "what are the customers",
    "what are all the clients", "what are all clients",
    "how many clients", "how many client", "how many customers", "how many customer",
    "total clients", "total client", "total customers", "total customer",
    "give me clients", "give me all clients", "give me the clients",
    "give me customers", "give me all customers",
    "tell me clients", "tell me all clients", "tell me the clients",
    "tell me customers", "tell me all customers",
    "get all clients", "get all customers",
    "can you list clients", "can you list customers",
    "can you show clients", "can you show customers",
}

# Intent words that signal "give me a count or listing" — covers every user phrasing
_LIST_INTENT_WORDS = [
    "list", "show", "tell", "give", "get", "all", "which", "what", "who",
    "how many", "display", "print", "enumerate", "count", "number of",
    "total number", "how much", "many", "unique", "different", "distinct",
]
# Words that specifically ask for the count (answers with count + list)
_COUNT_ONLY_WORDS = ["how many", "number of", "total number", "count of", "count msp", "count client"]

def _is_msp_list_query(q: str) -> bool:
    """Flexible MSP list detection — catches any phrasing including 'number of msp in Q2'."""
    q = q.lower().strip().rstrip("?.")
    if q in _LIST_MSP_KEYWORDS or any(kw in q for kw in _LIST_MSP_KEYWORDS):
        return True
    if "msp" in q and any(w in q for w in _LIST_INTENT_WORDS):
        return True
    if "msp" in q and any(w in q for w in ["in cycle", "in q", "in quarter", "in week", "cycle", "quarter", "week"]):
        return True
    return False

def _is_client_list_query(q: str) -> bool:
    """Flexible client/customer list detection — catches any phrasing."""
    q = q.lower().strip().rstrip("?.")
    if q in _LIST_CLIENT_KEYWORDS or any(kw in q for kw in _LIST_CLIENT_KEYWORDS):
        return True
    client_words = ["client", "clients", "customer", "customers", "account", "accounts"]
    if any(cw in q for cw in client_words) and any(w in q for w in _LIST_INTENT_WORDS):
        return True
    if any(cw in q for cw in client_words) and any(w in q for w in ["in cycle", "in q", "in quarter", "in week", "cycle", "quarter", "week"]):
        return True
    return False


# -------------------------------------------------
# LIST QUERY HANDLER — MSPs and Clients
# -------------------------------------------------
def _apply_list_filters(question: str) -> pd.DataFrame:
    """Apply cycle/quarter/week/company filters to df for list queries."""
    import re as _re
    working = df.copy()
    q = question.lower()

    col_map = build_column_map(df.columns)
    metadata = load_metadata("schema/metadata.json")

    # Company filter
    if "Customer Name" in df.columns:
        known = df["Customer Name"].dropna().str.lower().str.strip().unique()
        for c in sorted(known, key=len, reverse=True):
            if len(c) > 3 and c in q:
                real_col = resolve_dataframe_column("customer_name", col_map)
                if real_col:
                    mask = normalize_filter_value(real_col, c, working, metadata)
                    if mask is not None and mask.any():
                        working = working[mask]
                break

    # Week filter
    week_m = _re.search(r"week\s*0?(\d{1,2})", q)
    if week_m:
        n = int(week_m.group(1))
        real_col = resolve_dataframe_column("week_num", col_map)
        if real_col:
            mask = normalize_filter_value(real_col, f"Week {n:02d}", working, metadata)
            if mask is not None and mask.any():
                working = working[mask]

    # Cycle filter
    cycle_m = _re.search(r"cycle\s*0?(\d{1,2})", q)
    if cycle_m:
        n = int(cycle_m.group(1))
        real_col = resolve_dataframe_column("cycle", col_map)
        if real_col:
            mask = normalize_filter_value(real_col, f"Cycle {n:02d}", working, metadata)
            if mask is not None and mask.any():
                working = working[mask]

    # Quarter filter
    q_m = _re.search(r"q([1-4])", q)
    if q_m:
        real_col = resolve_dataframe_column("quarter", col_map)
        if real_col:
            mask = normalize_filter_value(real_col, f"Q{q_m.group(1)}", working, metadata)
            if mask is not None and mask.any():
                working = working[mask]

    return working


def handle_list_msps(question: str) -> str:
    """List all unique MSPs, optionally filtered by cycle/quarter/week."""
    working = _apply_list_filters(question)
    msp_col = None
    for candidate in ["M.S.P", "MSP", "Msp", "msp"]:
        if candidate in working.columns:
            msp_col = candidate
            break
    if not msp_col:
        return "MSP column not found in the data."

    _BAD_VALUES = {"nan", "none", "0", "0.0", "n/a", "na", "-", ""}
    msps = (
        working[msp_col]
        .dropna()
        .astype(str)
        .str.strip()
        .replace("", pd.NA)
        .dropna()
    )
    msps = msps[~msps.str.lower().isin(_BAD_VALUES)]
    msps = msps.value_counts()

    if msps.empty:
        return "No MSP data found for the given filters."

    # Build period label from question
    q = question.lower()
    period_label = ""
    _m = _re.search(r"cycle\s*0?(\d{1,2})", q)
    if _m: period_label = f" in Cycle {int(_m.group(1)):02d}"
    _m = _re.search(r"\bq([1-4])\b", q)
    if _m: period_label = f" in Q{_m.group(1)}"
    _m = _re.search(r"week\s*0?(\d{1,2})", q)
    if _m: period_label = f" in Week {int(_m.group(1)):02d}"

    # Count-intent queries: lead with the number prominently
    if any(w in q for w in _COUNT_ONLY_WORDS):
        header = f"There {'is' if len(msps) == 1 else 'are'} **{len(msps)} unique MSP{'s' if len(msps) != 1 else ''}**{period_label}:"
        return header + "\n\n" + "\n".join(f"- {m}" for m in msps.index)

    lines = [f"**Total MSPs: {len(msps)}**{period_label}", ""]
    for msp in msps.index:
        lines.append(f"- {msp}")
    return "\n".join(lines)



def handle_list_clients(question: str) -> str:
    """List all unique clients/customers, optionally filtered by cycle/quarter/week."""
    working = _apply_list_filters(question)
    cust_col = None
    for candidate in ["Customer Name", "customer name", "Client"]:
        if candidate in working.columns:
            cust_col = candidate
            break
    if not cust_col:
        return "Customer Name column not found in the data."

    _BAD_VALUES = {"nan", "none", "0", "0.0", "n/a", "na", "-", ""}
    clients = (
        working[cust_col]
        .dropna()
        .astype(str)
        .str.strip()
        .replace("", pd.NA)
        .dropna()
    )
    clients = clients[~clients.str.lower().isin(_BAD_VALUES)]
    clients = clients.value_counts()

    if clients.empty:
        return "No client data found for the given filters."

    q = question.lower()
    period_label = ""
    _m = _re.search(r"cycle\s*0?(\d{1,2})", q)
    if _m: period_label = f" in Cycle {int(_m.group(1)):02d}"
    _m = _re.search(r"\bq([1-4])\b", q)
    if _m: period_label = f" in Q{_m.group(1)}"
    _m = _re.search(r"week\s*0?(\d{1,2})", q)
    if _m: period_label = f" in Week {int(_m.group(1)):02d}"

    # Count-intent queries: lead with the count prominently
    if any(w in q for w in _COUNT_ONLY_WORDS):
        header = f"There {'is' if len(clients) == 1 else 'are'} **{len(clients)} unique client{'s' if len(clients) != 1 else ''}**{period_label}:"
        return header + "\n\n" + "\n".join(f"- {c}" for c in sorted(clients.index))

    lines = [f"**Total Clients: {len(clients)}**{period_label}", ""]
    for client in sorted(clients.index):
        lines.append(f"- {client}")
    return "\n".join(lines)



# -------------------------------------------------
# WORKER NAME RESOLVER
# Checks if any known worker name appears in the question
# -------------------------------------------------
def _find_worker_in_question(question: str) -> str | None:
    """
    Scans the question for a known worker name from the dataset.
    Returns the matched worker name or None.
    """
    try:
        if "Worker Name" not in df.columns:
            return None
        q_lower = question.lower()
        workers = df["Worker Name"].dropna().unique()
        # Try full name match first
        for w in workers:
            if str(w).lower() in q_lower:
                return str(w)
        # Try last name match
        for w in workers:
            parts = str(w).split()
            if len(parts) >= 2:
                last = parts[-1].lower()
                if len(last) > 3 and last in q_lower:
                    return str(w)
        return None
    except Exception:
        return None


# -------------------------------------------------
# QUESTION NORMALIZER
# Converts any way of saying cycle/quarter/week into canonical form
# e.g. "second cycle" → "Cycle 02", "third quarter" → "Q3", "week five" → "Week 05"
# -------------------------------------------------
import re as _re

_WORD_NUM = {
    "one":1,"first":1,"1st":1,
    "two":2,"second":2,"2nd":2,
    "three":3,"third":3,"3rd":3,
    "four":4,"fourth":4,"4th":4,
    "five":5,"fifth":5,"5th":5,
    "six":6,"sixth":6,"6th":6,
    "seven":7,"seventh":7,"7th":7,
    "eight":8,"eighth":8,"8th":8,
    "nine":9,"ninth":9,"9th":9,
    "ten":10,"tenth":10,"10th":10,
    "eleven":11,"eleventh":11,"11th":11,
    "twelve":12,"twelfth":12,"12th":12,
}

def _to_num(token: str):
    t = token.strip().lower()
    if t in _WORD_NUM: return _WORD_NUM[t]
    try: return int(t)
    except ValueError: return None

def _normalize_question(question: str) -> str:
    q = question

    # "cycle <N/word>" / "billing cycle <N>"
    q = _re.sub(r'\b(?:billing\s+)?cycle\s+(\w+)\b',
        lambda m: f"Cycle {_to_num(m.group(1)):02d}" if _to_num(m.group(1)) else m.group(0),
        q, flags=_re.IGNORECASE)

    # "<N/word> cycle" e.g. "second cycle"
    q = _re.sub(r'\b(\w+)\s+cycle\b',
        lambda m: f"Cycle {_to_num(m.group(1)):02d}" if _to_num(m.group(1)) else m.group(0),
        q, flags=_re.IGNORECASE)

    # "c<N>" standalone e.g. c3
    q = _re.sub(r'\bc(\d{1,2})\b',
        lambda m: f"Cycle {int(m.group(1)):02d}",
        q, flags=_re.IGNORECASE)

    # "quarter <N/word>" / "q<N>"
    q = _re.sub(r'\b(?:quarter\s+|q\s*)(\w+)\b',
        lambda m: f"Q{_to_num(m.group(1))}" if _to_num(m.group(1)) else m.group(0),
        q, flags=_re.IGNORECASE)

    # "<N/word> quarter"
    q = _re.sub(r'\b(\w+)\s+quarter\b',
        lambda m: f"Q{_to_num(m.group(1))}" if _to_num(m.group(1)) else m.group(0),
        q, flags=_re.IGNORECASE)

    # "week <N/word>" / "wk<N>"
    q = _re.sub(r'\b(?:week|wk)\s*(\w+)\b',
        lambda m: f"Week {_to_num(m.group(1)):02d}" if _to_num(m.group(1)) else m.group(0),
        q, flags=_re.IGNORECASE)

    return q


# -------------------------------------------------
# DERIVED MEASURE COMPUTER
# Handles metrics that are computed from multiple columns
# e.g. avg_bill_rate = Total Revenue / Total Hours
# -------------------------------------------------
def _compute_derived_measure(intent: dict, question: str) -> str | None:
    """
    Handles metrics computed from multiple columns.
    avg_bill_rate = Total Revenue / Total Hours  ($/hr)
    """
    measures = intent.get("measures", [])
    filters  = intent.get("filters", {})
    if not measures:
        return None

    measure = measures[0]
    q_lower = question.lower()

    BR_KEYWORDS = [
        "avg br", "avg br/hr", "average br", "average bill rate",
        "bill rate per hour", "br per hour", "br/hr",
        "avg billing rate", "average billing rate",
        "avg bill rate per hour", "bill rate per hr",
    ]
    is_br_per_hour = measure == "bill_rate_reg" and any(k in q_lower for k in BR_KEYWORDS)

    if not is_br_per_hour:
        return None

    try:
        # Apply filters to get working subset
        metadata   = load_metadata("schema/metadata.json")
        column_map = build_column_map(df.columns)
        working_df = df.copy()

        skip = {"all", "all cycles", "overall", ""}
        for col, val in filters.items():
            if str(val).lower().strip() in skip:
                continue
            real_col = resolve_dataframe_column(col, column_map)
            if not real_col:
                continue
            mask = normalize_filter_value(real_col, val, working_df, metadata)
            if mask is not None and mask.any():
                working_df = working_df[mask]

        if working_df.empty:
            return None

        # Avg BR/HR = Total Revenue / Total Hours
        rev_col   = None
        hours_col = None
        for candidate in ["Revenue", "Total Revenue"]:
            for col in working_df.columns:
                if col.strip() == candidate:
                    rev_col = col
                    break
            if rev_col: break

        for candidate in ["Total Hours"]:
            for col in working_df.columns:
                if col.strip() == candidate:
                    hours_col = col
                    break
            if hours_col: break

        if not rev_col or not hours_col:
            return None

        total_rev   = pd.to_numeric(working_df[rev_col],   errors="coerce").sum()
        total_hours = pd.to_numeric(working_df[hours_col], errors="coerce").sum()

        if total_hours == 0:
            return None

        avg_br_hr = total_rev / total_hours

        # Build context — worker takes priority over company as the primary subject
        worker   = filters.get("worker_name", "")
        company  = filters.get("customer_name", "")
        cycle    = filters.get("cycle", "")
        quarter  = filters.get("quarter", "")
        week     = filters.get("week_num", "")
        location = filters.get("location_code", "")

        parts = []
        if worker:    parts.append(f"for {worker}")
        elif company: parts.append(f"for {company}")
        if cycle:     parts.append(f"in {cycle}")
        if quarter:   parts.append(f"in {quarter}")
        if week:      parts.append(f"in {week}")
        if location:  parts.append(f"in {location}")
        context = " ".join(parts)

        formatted = f"${avg_br_hr:,.2f}/hr"
        if context:
            return f"The Avg Bill Rate (Revenue/Hours) {context} is **{formatted}**."
        return f"The Avg Bill Rate (Revenue/Hours) is **{formatted}**."

    except Exception as e:
        print(f"[DERIVED MEASURE ERROR] {e}")
        return None


# -------------------------------------------------
# DETERMINISTIC ANSWER FORMATTER
# Turns raw numbers into readable sentences
# -------------------------------------------------
def _format_deterministic(raw, intent: dict, question: str) -> str:
    """Convert a raw number/dict from the executor into a clean sentence."""
    measures  = intent.get("measures", [])
    filters   = intent.get("filters", {})
    company   = filters.get("customer_name", "")
    cycle     = filters.get("cycle", "")
    quarter   = filters.get("quarter", "")
    location  = filters.get("location_code", "")
    worker    = filters.get("worker_name", "")
    week      = filters.get("week_num", "")

    # Normalize filter display values
    def _norm_filter_display(val, prefix=""):
        if not val: return ""
        v = str(val).strip()
        # Pad bare numbers: "1" → "Cycle 01" / "Q1" / "Week 01"
        if prefix == "Cycle" and v.isdigit():
            return f"Cycle {int(v):02d}"
        if prefix == "Q" and v.isdigit():
            return f"Q{v}"
        if prefix == "Week" and v.isdigit():
            return f"Week {int(v):02d}"
        # Already prefixed
        if not v.startswith(prefix):
            return f"{prefix} {v}" if prefix else v
        return v

    cycle_disp   = _norm_filter_display(cycle, "Cycle")
    quarter_disp = _norm_filter_display(quarter, "Q")
    week_disp    = _norm_filter_display(week, "Week")

    # Resolve company name from df — exact match first, then partial
    company_disp = company
    if company and "Customer Name" in df.columns:
        all_companies = df["Customer Name"].dropna().unique()
        # Exact match first
        exact = [c for c in all_companies if c.lower().strip() == company.lower().strip()]
        if exact:
            company_disp = exact[0]
        else:
            # Partial match — longest wins
            matches = sorted(
                [c for c in all_companies if company.lower() in c.lower() or c.lower() in company.lower()],
                key=len, reverse=True
            )
            if matches:
                company_disp = matches[0]

    # Build context string — worker takes priority over company as the primary subject
    # e.g. "for Alexis Cruz in Cycle 03" / "for Abbott Labs in Q2"
    context_parts = []
    if worker:
        context_parts.append(f"for {worker}")
    elif company_disp:
        context_parts.append(f"for {company_disp}")
    if cycle_disp:   context_parts.append(f"in {cycle_disp}")
    if quarter_disp: context_parts.append(f"in {quarter_disp}")
    if week_disp:    context_parts.append(f"in {week_disp}")
    if location:     context_parts.append(f"in {location}")
    context = " ".join(context_parts)

    # Handle dict/DataFrame result
    if isinstance(raw, dict):
        lines = [f"{k}: {_fmt_value(k, v)}" for k, v in raw.items()]
        return "\n".join(lines)


    # Handle single numeric value
    try:
        val = float(raw)
    except (TypeError, ValueError):
        return str(raw)

    measure = measures[0] if measures else "value"

    # Format based on measure type
    CURRENCY_MEASURES = {
        "revenue", "gm_dollars", "base_cost", "total_cost",
        "net_revenue", "vms_fees", "loaded_cost",
        "pay_rate_reg", "pay_rate_ot", "pay_rate_dt",
        "bill_rate_reg", "bill_rate_ot", "bill_rate_dt",
        "spread", "markup", "avg_bill_rate", "avg_pay_rate",
        "bill_rate", "pay_rate", "billing_rate",
    }
    PCT_MEASURES = {"gm_pct", "markup_pct", "vms_pct", "load_factor_pct"}
    HOUR_MEASURES = {"total_hours", "reg_hours", "ot_hours", "dt_hours"}

    if measure == "headcount":
        formatted = f"{int(val):,} workers"
    elif measure in CURRENCY_MEASURES:
        formatted = _M(val)
    elif measure in PCT_MEASURES:
        formatted = _P(val)
    elif measure in HOUR_MEASURES:
        formatted = f"{val:,.1f} hours"
    else:
        formatted = f"{val:,.2f}"

    # Map measure to human label
    LABELS = {
        "headcount":     "Total Headcount (Unique Workers)",
        "revenue":       "Total Revenue",
        "gm_dollars":    "Gross Margin ($)",
        "gm_pct":        "Gross Margin %",
        "base_cost":     "Base Cost",
        "total_hours":   "Total Hours",
        "bill_rate_reg": "Avg Bill Rate",
        "bill_rate_ot":  "Avg OT Bill Rate",
        "bill_rate_dt":  "Avg DT Bill Rate",
        "bill_rate":     "Avg Bill Rate",
        "billing_rate":  "Avg Bill Rate",
        "pay_rate_reg":  "Avg Pay Rate",
        "pay_rate_ot":   "Avg OT Pay Rate",
        "pay_rate_dt":   "Avg DT Pay Rate",
        "pay_rate":      "Avg Pay Rate",
        "spread":        "Avg Spread",
        "reg_hours":     "Regular Hours",
        "ot_hours":      "Overtime Hours",
        "dt_hours":      "Double Time Hours",
        "gm_dollars":    "Gross Margin ($)",
        "net_revenue":   "Net Revenue",
        "loaded_cost":   "Loaded Cost",
        "vms_fees":      "VMS Fees",
    }
    label = LABELS.get(measure, measure.replace("_", " ").title())

    if context:
        return f"The {label} {context} is **{formatted}**."
    return f"The {label} is **{formatted}**."


def _fmt_value(key: str, val) -> str:
    """Format a single value based on its key name."""
    if val is None: return "N/A"
    k = key.lower()
    try:
        v = float(val)
        if any(x in k for x in ["revenue", "cost", "gm$", "margin$", "fee", "rate", "spread"]):
            return _M(v)
        if any(x in k for x in ["pct", "%", "percent"]):
            return _P(v)
        if "hour" in k:
            return f"{v:,.1f}"
        return f"{v:,.2f}"
    except (TypeError, ValueError):
        return str(val)


# -------------------------------------------------
# AVG HOURS PER WORKER PER PERIOD
# Formula: Total Hours in period / Unique Workers in period
# Handles: cycle, quarter, week — specific or breakdown
# -------------------------------------------------
_AVG_HRS_WORKER_KW = [
    "avg hrs per", "average hrs per", "avg hours per", "average hours per",
    "hrs per worker", "hours per worker", "hours per employee",
    "hours per associate", "hours per headcount", "hours per person",
    "hrs per employee", "hrs per associate", "hrs per headcount", "hrs per person",
    "avg hrs/worker", "avg hrs/employee", "avg hrs/associate", "avg hrs/headcount",
    "average hrs/worker", "average hrs/employee",
    "hours/worker", "hours/employee", "hours/associate",
]

def handle_avg_hrs_per_worker(question: str, session_id: str) -> str | None:
    """
    Computes Total Hours / Unique Workers for a given period or as a breakdown.
    Returns a formatted string or None if not applicable.
    """
    q = question.lower()
    if not any(kw in q for kw in _AVG_HRS_WORKER_KW):
        return None

    # Determine period dimension
    period_col   = None
    period_filter = None
    period_label  = None

    if "cycle" in q:
        period_label = "Cycle"
        # Try to find the actual column name
        for candidate in ["Cycle ", "Cycle"]:
            if candidate in df.columns:
                period_col = candidate
                break
        m = _re.search(r"cycle\s*0?(\d{1,2})", q)
        if m:
            period_filter = f"Cycle {int(m.group(1)):02d}"

    elif "quarter" in q or _re.search(r"\bq[1-4]\b", q):
        period_label = "Quarter"
        if "Quarter" in df.columns:
            period_col = "Quarter"
        m = _re.search(r"\bq([1-4])\b", q)
        if m:
            period_filter = f"Q{m.group(1)}"

    elif "week" in q:
        period_label = "Week"
        for candidate in ["Week #", "Week Num", "Week"]:
            if candidate in df.columns:
                period_col = candidate
                break
        m = _re.search(r"week\s*0?(\d{1,2})", q)
        if m:
            period_filter = f"Week {int(m.group(1)):02d}"

    if "Total Hours" not in df.columns or "Worker Name" not in df.columns:
        return None

    # Apply company filter from session or question
    working = df.copy()
    company  = _session_company.get(session_id)
    metadata = load_metadata("schema/metadata.json")
    col_map  = build_column_map(df.columns)

    if company:
        real_col = resolve_dataframe_column("customer_name", col_map)
        if real_col:
            mask = normalize_filter_value(real_col, company, working, metadata)
            if mask is not None and mask.any():
                working = working[mask]

    def _compute_avg(sub: pd.DataFrame) -> tuple[float, float, int]:
        """Returns (avg, total_hours, unique_workers)"""
        hrs = pd.to_numeric(sub["Total Hours"], errors="coerce").sum()
        wkrs = sub["Worker Name"].dropna().str.strip().str.lower().nunique()
        avg  = round(hrs / wkrs, 1) if wkrs > 0 else 0.0
        return avg, hrs, wkrs

    # ── Specific period requested ─────────────────────────────────────
    if period_filter and period_col:
        mask = normalize_filter_value(period_col, period_filter, working, metadata)
        if mask is None or not mask.any():
            return f"No data found for {period_filter}."
        sub = working[mask]
        avg, hrs, wkrs = _compute_avg(sub)
        ctx = f"in {period_filter}"
        if company:
            ctx = f"for {company} {ctx}"
        return (
            f"**Avg Hours per Worker** {ctx}: **{avg:,.1f} hrs/worker**\n"
            f"({hrs:,.1f} total hours ÷ {wkrs} unique workers)"
        )

    # ── Breakdown by period ───────────────────────────────────────────
    if period_col and period_col in working.columns:
        rows = []
        for period_val, grp in working.groupby(period_col):
            avg, hrs, wkrs = _compute_avg(grp)
            rows.append({"period": period_val, "avg": avg, "hrs": hrs, "wkrs": wkrs})

        if not rows:
            return "No data found."

        # Sort by period value
        try:
            rows.sort(key=lambda r: str(r["period"]))
        except Exception:
            pass

        header = f"**Avg Hours per Worker by {period_label}**"
        if company:
            header += f" — {company}"
        lines = [header, ""]
        for r in rows:
            lines.append(f"- **{r['period']}:** {r['avg']:,.1f} hrs  ({r['hrs']:,.0f} hrs ÷ {r['wkrs']} workers)")
        return "\n".join(lines)

    # ── No period — overall ───────────────────────────────────────────
    avg, hrs, wkrs = _compute_avg(working)
    ctx = f"for {company}" if company else "(overall)"
    return (
        f"**Avg Hours per Worker** {ctx}: **{avg:,.1f} hrs/worker**\n"
        f"({hrs:,.1f} total hours ÷ {wkrs} unique workers)"
    )


# -------------------------------------------------
# UPLOAD ENDPOINT
# -------------------------------------------------
@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    filename = file.filename or "upload.xlsx"

    if not filename.lower().endswith((".xlsx", ".xls")):
        raise HTTPException(status_code=400, detail="Only .xlsx or .xls files are supported.")

    excel_bytes = await file.read()
    if not excel_bytes:
        raise HTTPException(status_code=400, detail="Uploaded file is empty.")

    try:
        result = process_uploaded_file(excel_bytes, filename)
        reload_data()
        return {
            "status":     "success",
            "message":    f"'{filename}' loaded successfully. Bot is ready.",
            "rows":        result["rows"],
            "columns":     result["columns"],
            "measures":    result["measures"],
            "dimensions":  result["dimensions"],
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# -------------------------------------------------
# ASK ENDPOINT
# -------------------------------------------------
@app.post("/ask")
def ask(question: str, session_id: str = "default", model: str | None = None):
    try:
        # Step 1: Normalize question (cycle/quarter/week word forms)
        normalized = _normalize_question(question)

        # Step 1b: Generic company references → replace with session company
        company = _session_company.get(session_id)
        if company:
            import re as _re2
            generic_patterns = [
                r'the company', r'this company', r'this client',
                r'the client', r'the account', r'this account',
            ]
            for pat in generic_patterns:
                if _re2.search(pat, normalized, _re2.IGNORECASE):
                    normalized = _re2.sub(pat, company, normalized, flags=_re2.IGNORECASE)
                    break

        # Step 1c: "spectraforce" = our company = overall totals
        # Strip it from the question and suppress session company injection for this call
        if any(term in normalized.lower() for term in _OWN_COMPANY_TERMS):
            normalized = _re.sub(r'\bspectraforce\b', '', normalized, flags=_re.IGNORECASE).strip()
            company = None   # local override — session is NOT cleared

        # Step 1d: Compare queries — "compare X in cycle 3 and cycle 4"
        compare_result = _handle_compare(normalized, session_id, model)
        if compare_result:
            append_history(session_id, "user", question)
            append_history(session_id, "assistant", compare_result)
            return {"question": question, "session_id": session_id, "answer": compare_result}

        # Step 2: Parse intent
        intent = parse_intent("", normalized)
        q_lower = normalized.lower()

        # Step 2b: List queries — MSPs and Clients
        _LIST_MSP_KEYWORDS    = ["list msp", "list all msp", "all msps", "show msp",
                                  "show all msp", "list msps", "msps", "msp list",
                                  "which msps", "what msps", "how many msps",
                                  "all msp", "list the msp"]
        _LIST_CLIENT_KEYWORDS = ["list client", "list all client", "all clients",
                                  "show clients", "list customers", "all customers",
                                  "how many clients", "how many customers",
                                  "total clients", "total customers", "list the client",
                                  "which clients", "what clients", "who are the clients"]

        if any(kw in q_lower for kw in _LIST_MSP_KEYWORDS):
            answer = handle_list_msps(normalized)
            append_history(session_id, "user", question)
            append_history(session_id, "assistant", answer)
            return {"question": question, "session_id": session_id, "answer": answer}

        if any(kw in q_lower for kw in _LIST_CLIENT_KEYWORDS):
            answer = handle_list_clients(normalized)
            append_history(session_id, "user", question)
            append_history(session_id, "assistant", answer)
            return {"question": question, "session_id": session_id, "answer": answer}

        # Step 2c: Avg hours per worker per period
        avg_hrs_answer = handle_avg_hrs_per_worker(normalized, session_id)
        if avg_hrs_answer:
            append_history(session_id, "user", question)
            append_history(session_id, "assistant", avg_hrs_answer)
            return {"question": question, "session_id": session_id, "answer": avg_hrs_answer}

        # Step 3: Company full profile
        if intent.get("type") == "company_full_profile":
            cust = intent.get("filters", {}).get("customer_name", "")
            generic_names = {
                "company", "the company", "this company", "that company",
                "it", "them", "the client", "client", "account", "the account",
                "this client", "this account", "our client", "our account",
                "same company", "same client",
            }
            if cust.lower().strip() in generic_names or len(cust.strip()) <= 3:
                session_cust = _session_company.get(session_id)
                if session_cust:
                    intent["filters"]["customer_name"] = session_cust
                    print(f"[SESSION INJECT PROFILE] Using session company: {session_cust}")
                else:
                    return {"question": question, "session_id": session_id,
                            "answer": "Please mention the company name — for example: *'tell me about AbbVie'*."}

            profile_data = execute(intent, df, schema)
            # New company loaded — clear stale worker session
            _session_worker.pop(session_id, None)
            answer = handle_company_profile(profile_data, normalized, session_id, model)
            return {"question": question, "session_id": session_id, "intent": intent, "answer": answer, "data": profile_data}

        # Step 3.5: Compare questions
        company = _session_company.get(session_id)   # re-fetch (may have been suppressed above)
        compare_answer = _handle_compare_question(normalized, question, intent, session_id, model)
        if compare_answer:
            append_history(session_id, "user", question)
            append_history(session_id, "assistant", compare_answer)
            return {"question": question, "session_id": session_id, "answer": compare_answer}

        # Step 4: Deterministic path
        deterministic_answer = None

        if intent.get("measures"):
            _resolve_filters(intent)

            # ── Worker follow-up injection ──────────────────────────────────
            # If a worker was the last queried entity, ALL follow-up questions
            # (without an explicit entity) stay on that worker — no pronouns needed.
            # e.g. after "revenue of Alexis Cruz", asking "avg bill rate in week 5"
            # should return Alexis Cruz's bill rate, not the company's.
            session_worker = _session_worker.get(session_id)
            if (session_worker
                    and not intent.get("filters", {}).get("worker_name")
                    and not intent.get("filters", {}).get("customer_name")
                    and not _is_total_query(normalized)):
                intent.setdefault("filters", {})["worker_name"] = session_worker
                print(f"[WORKER SESSION INJECT] {session_worker}")

            # ── Session company injection ───────────────────────────────────
            # Only runs when no worker session is active
            if company and not intent.get("filters", {}).get("customer_name"):
                if intent.get("filters", {}).get("worker_name"):
                    print(f"[SESSION INJECT SKIPPED] Worker query")
                elif _is_total_query(normalized):
                    print(f"[SESSION INJECT SKIPPED] Total/overall query — using full file")
                else:
                    known_companies = (df["Customer Name"].dropna().str.lower().str.strip().unique()
                                       if "Customer Name" in df.columns else [])
                    if not any(c in q_lower for c in known_companies if len(c) > 3):
                        intent.setdefault("filters", {})["customer_name"] = company
                        print(f"[SESSION INJECT] Added company filter: {company}")
                    else:
                        print(f"[SESSION INJECT SKIPPED] Question already names a company")

            # Track new company in filters
            resolved_company = intent.get("filters", {}).get("customer_name", "")
            if resolved_company and resolved_company.lower() not in {
                "company", "the company", "client", "account", "it", ""
            }:
                if resolved_company != _session_company.get(session_id):
                    _session_worker.pop(session_id, None)   # switched company → clear worker
                _session_company[session_id] = resolved_company

            # Worker scan fallback
            if not intent.get("filters"):
                worker_match = _find_worker_in_question(normalized)
                if worker_match:
                    intent.setdefault("filters", {})["worker_name"] = worker_match

            # Derived measures
            derived = _compute_derived_measure(intent, normalized)
            if derived is not None:
                print(f"[DERIVED HIT] {question!r}")
                deterministic_answer = derived
            else:
                try:
                    if "headcount" not in intent.get("measures", []):
                        validate_intent(intent, schema, normalized)
                    raw = execute(intent, df, schema)
                    print(f"[DETERMINISTIC HIT] {question!r}")
                    deterministic_answer = _format_deterministic(raw, intent, normalized)
                except Exception as e:
                    print(f"[DETERMINISTIC MISS] {e}")

            # Track worker session after a successful worker query
            # Also: if user named a DIFFERENT worker explicitly, update session immediately
            answered_worker = intent.get("filters", {}).get("worker_name", "")
            if answered_worker and deterministic_answer:
                if answered_worker != _session_worker.get(session_id):
                    print(f"[WORKER SESSION UPDATED] {_session_worker.get(session_id)!r} → {answered_worker!r}")
                _session_worker[session_id] = answered_worker
                # Worker context is now active — clear company from LLM context for this query
                # (company session itself is preserved for when user returns to company questions)

            # If question was a total/spectraforce query, clear worker session
            # so subsequent questions don't incorrectly inherit worker context
            if _is_total_query(normalized) and _session_worker.get(session_id):
                _session_worker.pop(session_id, None)
                print(f"[WORKER SESSION CLEARED] Total query — reverting to company context")

        # Step 5: LLM followup using session company fact sheet
        # Skip for worker queries — deterministic is authoritative there
        is_worker_query = bool(intent.get("filters", {}).get("worker_name"))

        if company and not is_worker_query:
            fact_sheet = get_fact_sheet(company)
            if fact_sheet:
                history = get_history(session_id)
                llm_answer = call_llm_followup(normalized, fact_sheet, history, model)

                if llm_answer:
                    DATA_MEASURE_WORDS = [
                        "revenue", "rev", "gm", "gross margin", "margin", "cost",
                        "bill rate", "br", "pay rate", "hours", "headcount",
                        "associates", "workers", "markup", "vms", "spread",
                        "profit", "total", "average", "avg", "how much", "how many",
                    ]
                    FORCE_LLM_PHRASES = [
                        "compare", "vs ", "versus", "difference between",
                        "and cycle", "and q", "and quarter", "and week",
                        "both", "all cycles", "across cycles",
                        "which is better", "which was best", "which was worst",
                        "trend", "over time", "progression",
                    ]
                    is_data_question = any(w in q_lower for w in DATA_MEASURE_WORDS)
                    force_llm        = any(p in q_lower for p in FORCE_LLM_PHRASES)
                    answer = deterministic_answer if (deterministic_answer and is_data_question and not force_llm) else llm_answer

                    append_history(session_id, "user", question)
                    append_history(session_id, "assistant", answer)
                    return {"question": question, "session_id": session_id, "answer": answer}

        # Deterministic only (worker query, no session company, or total query)
        if deterministic_answer:
            append_history(session_id, "user", question)
            append_history(session_id, "assistant", deterministic_answer)
            return {"question": question, "session_id": session_id, "answer": deterministic_answer}

        # Step 6: LLM fallback
        llm_output = ask_llm(schema, normalized, model)
        intent     = parse_intent(llm_output, normalized)
        validate_intent(intent, schema, normalized)
        raw    = execute(intent, df, schema)
        answer = _format_deterministic(raw, intent, normalized) if intent.get("measures") else str(raw)
        append_history(session_id, "user", question)
        append_history(session_id, "assistant", answer)
        return {"question": question, "session_id": session_id, "intent": intent, "answer": answer}

    except Exception as e:
        return {"question": question, "session_id": session_id, "error": str(e)}


# -------------------------------------------------
# UTILITY ENDPOINTS
# -------------------------------------------------
@app.post("/cache/clear")
def api_clear_all_cache():
    clear_cache()
    global _fact_sheet_cache, _fact_sheet_ts, _conversations, _session_company, _session_worker
    _fact_sheet_cache = {}; _fact_sheet_ts = {}; _conversations = {}
    _session_company  = {}; _session_worker = {}
    return {"status": "All caches and conversations cleared"}

@app.post("/cache/clear-customer")
def api_clear_customer_cache(customer_name: str):
    clear_customer_cache(customer_name)
    k = customer_name.lower().strip()
    _fact_sheet_cache.pop(k, None)
    _fact_sheet_ts.pop(k, None)
    return {"status": f"Cache cleared for {customer_name}"}

@app.post("/conversation/clear")
def api_clear_conversation(session_id: str = "default"):
    _conversations.pop(session_id, None)
    _session_company.pop(session_id, None)
    _session_worker.pop(session_id, None)
    return {"status": f"Conversation cleared for session {session_id}"}

@app.get("/customers")
def api_get_customers():
    customers = sorted(df["Customer Name"].dropna().unique().tolist())
    workers   = sorted(df["Worker Name"].dropna().unique().tolist()) if "Worker Name" in df.columns else []
    return {"customers": customers, "workers": workers}

@app.get("/health")
def api_health_check():
    return {"status": "healthy", "rows": len(df), "columns": len(df.columns)}