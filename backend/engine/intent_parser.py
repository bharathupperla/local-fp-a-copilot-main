import json
import re
from typing import Dict, Any
from engine.canonical_resolver import resolve_to_canonical

# ---------------------------------------------------------
# DEFAULT INTENT CONTRACT
# ---------------------------------------------------------
DEFAULT_INTENT = {
    "measures": [],
    "filters": {},
    "aggregation": "sum",
    "derived_logic": None,
    "dimensions": []
}

# ---------------------------------------------------------
# JSON EXTRACTION (LLM SAFETY)
# ---------------------------------------------------------
def _extract_json(text: str) -> Dict[str, Any]:
    if not isinstance(text, str) or not text.strip():
        return {}

    text = text.strip()
    text = re.sub(r"^```json|^```|```$", "", text, flags=re.IGNORECASE).strip()

    try:
        parsed = json.loads(text)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        pass

    match = re.search(r"\{.*\}", text, re.DOTALL)
    if match:
        try:
            parsed = json.loads(match.group())
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            pass

    return {}

# ---------------------------------------------------------
# MEASURE INFERENCE
# ---------------------------------------------------------
MEASURE_KEYWORDS = {
    # gm_dollars checked FIRST — "gross margin" without % means dollars
    "gm_dollars":    ["gross margin", "gm$", "gm dollars", "gm dollar", "total gm",
                      "gm generated", "margin generated", "margin made",
                      "gross margin dollar", "gross margin amount", "profit"],
    # gm_pct only when % or percent/pct explicitly present
    "gm_pct":        ["gm%", "gm pct", "gm percent", "gross margin %", "gross margin percent",
                      "gross margin percentage", "margin %", "margin percent", "margin percentage",
                      "gm percentage", "avg gm%", "average gm%", "gm perc"],
    "revenue":       ["revenue", "rev", "sales", "total revenue", "total rev"],
    "bill_rate_reg": ["bill rate", "br/hr", "bill rate per hour", "avg br", "average bill rate",
                      "billing rate", "avg billing rate", "bill rt", "billrate", "br "],
    "pay_rate_reg":  ["pay rate", "payrate", "pay rt", "avg pay", "average pay rate"],
    "total_hours":   ["total hours", "hours", "worked hours", "hrs"],
    "base_cost":     ["base cost", "cost", "total cost"],
    "reg_hours":     ["regular hours", "reg hours"],
    "ot_hours":      ["overtime hours", "ot hours"],
    "spread":        ["spread", "margin per hour"],
    "headcount":     ["how many", "count", "associates", "headcount", "workers", "employees",
                      "number of workers", "number of associates", "no of workers"],
}

def _infer_measure_from_question(question: str) -> str | None:
    q = question.lower()
    for measure, keywords in MEASURE_KEYWORDS.items():
        if any(k in q for k in keywords):
            return measure
    return None

# ---------------------------------------------------------
# COMPANY NAME EXTRACTION
# Handles: "Baxter (PR)", "BeiGene-PR", "The Goldman Sachs Group, Inc"
# ---------------------------------------------------------
COMPANY_PROFILE_PHRASES = [
    # "tell" variants
    "tell me about",
    "tell me everything about",
    "tell about",
    "tell me all about",
    # "details" variants
    "details about",
    "details of",
    "give me details",
    "full details",
    # "show" variants
    "show me about",
    "show me details",
    "show details",
    # "overview / summary" variants
    "company overview",
    "client overview",
    "customer overview",
    "overview of",
    "summary of",
    "summarize",
    "give me a summary",
    # "know / info" variants
    "what do we know about",
    "what do you know about",
    "info about",
    "information about",
    # "about / profile" variants
    "everything about",
    "all about",
    "profile of",
    "about the company",
    "about this company",
    "about the client",
    "about the account",
]

# Characters allowed in company names (includes brackets, dashes, commas, dots)
_COMPANY_CHARS = r"[a-z0-9\s&\-\.\,\(\)\/\'\"_]+"

def _extract_company_name(question: str) -> str | None:
    q = question.lower().strip()

    # Pattern 1: "about/of <company>" stopping at known trailing words
    match = re.search(
        r"(?:about|of)\s+(" + _COMPANY_CHARS + r")(?:\s+in\s+cycle|\s+in\s+q\d|\s+in\s+quarter|\s+in\s+week|\?|$)",
        q
    )
    if match:
        name = match.group(1).strip().rstrip(".,?")
        if len(name) > 1:
            return _title_company(name)

    # Pattern 2: after a known profile phrase
    for phrase in sorted(COMPANY_PROFILE_PHRASES, key=len, reverse=True):
        if phrase in q:
            remainder = q.split(phrase, 1)[-1].strip()
            remainder = re.sub(r"[?!]$", "", remainder).strip()
            if remainder and len(remainder) > 1:
                return _title_company(remainder)

    return None


def _title_company(name: str) -> str:
    """
    Smart title-case that preserves special chars like (PR), -PR, Inc.
    """
    # Capitalize first letter of each word but preserve special tokens
    words = name.strip().split()
    result = []
    for w in words:
        # Keep all-caps tokens like PR, LLC, INC
        if w.upper() == w and len(w) <= 5 and w.isalpha():
            result.append(w.upper())
        else:
            result.append(w.capitalize())
    return " ".join(result)


# ---------------------------------------------------------
# FILTER EXTRACTION — company, cycle, quarter, week, worker
# ---------------------------------------------------------
# Populated at startup by main.py with the actual company list from data
_KNOWN_COMPANIES_LOWER: list = []   # lowercase for matching
_KNOWN_COMPANIES_ORIG:  list = []   # original case for filter value

def set_known_companies(companies: list):
    global _KNOWN_COMPANIES_LOWER, _KNOWN_COMPANIES_ORIG
    _KNOWN_COMPANIES_ORIG  = list(companies)
    _KNOWN_COMPANIES_LOWER = [c.lower().strip() for c in companies]


# Worker list for matching in questions
_KNOWN_WORKERS_LOWER: list = []
_KNOWN_WORKERS_ORIG:  list = []

def set_known_workers(workers: list):
    global _KNOWN_WORKERS_LOWER, _KNOWN_WORKERS_ORIG
    _KNOWN_WORKERS_ORIG  = list(workers)
    _KNOWN_WORKERS_LOWER = [w.lower().strip() for w in workers]


# Worker list for fast lookup in worker detection
_KNOWN_WORKERS_LOWER: list = []
_KNOWN_WORKERS_ORIG:  list = []

def set_known_workers(workers: list):
    global _KNOWN_WORKERS_LOWER, _KNOWN_WORKERS_ORIG
    _KNOWN_WORKERS_ORIG  = list(workers)
    _KNOWN_WORKERS_LOWER = [w.lower().strip() for w in workers]


def _match_worker_anywhere(q: str, filters: dict) -> None:
    """
    Scans the entire question for any known worker name.
    Works without prepositions: "Adaeze revenue", "Godwin Bongham gm in cycle 3"
    """
    global _KNOWN_WORKERS_LOWER, _KNOWN_WORKERS_ORIG
    if not _KNOWN_WORKERS_LOWER:
        return

    q_lower = q.lower().strip()
    best_idx = -1
    best_len = 0

    for i, worker in enumerate(_KNOWN_WORKERS_LOWER):
        if len(worker) > 4 and worker in q_lower and len(worker) > best_len:
            best_idx = i
            best_len = len(worker)

    if best_idx >= 0:
        original = _KNOWN_WORKERS_ORIG[best_idx]
        filters["worker_name"] = original
        print(f"[WORKER SCAN] Matched: '{original}'")


def _match_company_anywhere(q: str, filters: dict) -> None:
    """
    Scans the ENTIRE question for any known company name.
    No dependency on sentence structure, prepositions, or word order.
    Picks the longest match to avoid false positives.
    e.g. "BMS CART Devens gm" -> BMS CART Devens
         "give abbott labs the revenue" -> Abbott Labs
         "what did abbvie make in q2" -> AbbVie
    """
    if not _KNOWN_COMPANIES_LOWER:
        return

    # "spectraforce" = our own company = overall totals, never a client filter
    _OWN_COMPANY_TERMS = {"spectraforce", "spectraforce total", "spectraforce overall"}
    if any(term in q.lower() for term in _OWN_COMPANY_TERMS):
        return

    q_lower = q.lower().strip()
    best_idx = -1
    best_len = 0

    for i, company in enumerate(_KNOWN_COMPANIES_LOWER):
        # Only match companies with more than 2 chars to avoid false positives
        if len(company) > 2 and company in q_lower and len(company) > best_len:
            best_idx = i
            best_len = len(company)

    if best_idx >= 0:
        original = _KNOWN_COMPANIES_ORIG[best_idx]
        filters["customer_name"] = original
        print(f"[COMPANY SCAN] Matched: '{original}' (len={best_len})")


def _extract_filters_from_question(question: str) -> Dict[str, Any]:
    filters: Dict[str, Any] = {}
    q = question.lower()

    # ---------------------------
    # WORKER NAME — detect FIRST using original case (proper nouns)
    # e.g. "revenue of Godwin Bongham in Q1" -> worker_name=Godwin Bongham, quarter=Q1
    # ---------------------------
    # Worker regex — works with any case (user may type lowercase)
    worker_match = re.search(
        r"(?:of|for|by|made by|earned by|did)\s+([a-zA-Z][a-z]+(?:\s+[a-zA-Z][a-z]+){1,4})"
        r"(?:\s+in\s+|\s+for\s+|\s+during\s+|\s+earn|\s+make|\?|$)",
        question,
        re.IGNORECASE
    )
    if not worker_match:
        worker_match = re.search(
            r"(?:of|for|by|made by|earned by)\s+([a-zA-Z][a-z]+(?:\s+[a-zA-Z][a-z]+){1,4})\s*$",
            question.rstrip("?.,!"),
            re.IGNORECASE
        )
    if worker_match:
        candidate = worker_match.group(1).strip()
        candidate_lower = candidate.lower().strip()

        # Check against known company list first — if it's a company, skip
        is_known_company = any(
            candidate_lower == c or candidate_lower in c or c in candidate_lower
            for c in _KNOWN_COMPANIES_LOWER if len(c) > 3
        )

        # If known workers list is populated, verify candidate is actually a worker
        if _KNOWN_WORKERS_LOWER and not is_known_company:
            is_known_worker = any(
                candidate_lower == w or w.startswith(candidate_lower)
                for w in _KNOWN_WORKERS_LOWER
            )
            if not is_known_worker:
                # Not in worker list — skip
                worker_match = None

        company_indicators = [
            "inc", "llc", "corp", "group", "labs", "ltd", "co.", "solutions",
            "pharma", "health", "bank", "capital", "partners", "associates",
            "services", "technologies", "systems", "global", "international",
            "management", "consulting", "staffing", "holdings", "enterprises",
            "culinary", "medical", "clinical", "scientific", "financial",
            "cart", "devens", "bms",
        ]
        is_company_indicator = any(ind in candidate_lower for ind in company_indicators)
        word_count = len(candidate.split())

        if not is_known_company and not is_company_indicator and 2 <= word_count <= 4:
            filters["worker_name"] = candidate.title()

    # ---------------------------
    # CUSTOMER NAME — scan entire question for any known company name
    # Works regardless of sentence structure, word order, or prepositions
    # ---------------------------
    if "worker_name" not in filters:
        _match_company_anywhere(q, filters)

    # ---------------------------
    # WORKER NAME — scan entire question for any known worker name
    # Works even without of/for/by prepositions: "Adaeze revenue", "Godwin Bongham gm"
    # ---------------------------
    if "worker_name" not in filters and "customer_name" not in filters:
        _match_worker_anywhere(q, filters)

    # ---------------------------
    # CYCLE
    # ---------------------------
    if "all cycle" not in q and "all cycles" not in q:
        cycle_match = re.search(r"cycle\s*0?(\d{1,2})", q)
        if cycle_match:
            n = int(cycle_match.group(1))
            filters["cycle"] = f"Cycle {n:02d}"

    # ---------------------------
    # QUARTER
    # ---------------------------
    q_match = re.search(r"\bq([1-4])\b", q)
    if q_match:
        filters["quarter"] = f"Q{q_match.group(1)}"

    # ---------------------------
    # WEEK
    # ---------------------------
    week_match = re.search(r"week\s*0?(\d{1,2})", q)
    if week_match:
        n = int(week_match.group(1))
        filters["week_num"] = f"Week {n:02d}"

    return filters


# ---------------------------------------------------------
# AGGREGATION INFERENCE
# ---------------------------------------------------------
def _infer_aggregation(question: str, measure: str | None) -> str:
    q = question.lower()

    if any(k in q for k in ["average", "avg", "mean"]):
        return "avg"

    if any(k in q for k in ["how many", "count", "number of"]):
        return "count"

    if measure == "gm_pct":
        return "avg"

    if measure in ("bill_rate_reg", "pay_rate_reg", "bill_rate_ot",
                   "pay_rate_ot", "bill_rate_dt", "pay_rate_dt", "spread"):
        return "avg"

    return "sum"


# ---------------------------------------------------------
# MAIN PARSER
# ---------------------------------------------------------
def parse_intent(llm_output: str, question: str) -> Dict[str, Any]:

    q = question.lower().strip()

    # -----------------------------------------------
    # COMPANY FULL PROFILE INTENT (PRIORITY)
    # -----------------------------------------------
    if any(phrase in q for phrase in COMPANY_PROFILE_PHRASES):
        company_name = _extract_company_name(question)
        if company_name:
            return {
                "type": "company_full_profile",
                "filters": {
                    "customer_name": company_name
                }
            }

    # -----------------------------------------------
    # MEASURE-BASED INTENT
    # -----------------------------------------------
    parsed = _extract_json(llm_output)

    intent = {
        "measures": [],
        "filters": {},
        "aggregation": "sum",
        "derived_logic": None,
        "dimensions": []
    }

    for k, v in parsed.items():
        intent[k] = v

    # Normalize filters from LLM
    raw_filters: Dict[str, Any] = {}
    if isinstance(intent.get("filter"), dict):
        raw_filters.update(intent["filter"])
    if isinstance(intent.get("filters"), dict):
        raw_filters.update(intent["filters"])

    intent["filters"] = {}
    for k, v in raw_filters.items():
        canonical = resolve_to_canonical(k)
        if canonical and v not in (None, "", "s"):
            intent["filters"][canonical] = v

    # Deterministic fallback filters (always run)
    extracted = _extract_filters_from_question(question)
    for k, v in extracted.items():
        intent["filters"].setdefault(k, v)

    # Resolve measures
    resolved_measures = []
    if isinstance(intent.get("measures"), list):
        for m in intent["measures"]:
            canonical = resolve_to_canonical(m)
            if canonical:
                resolved_measures.append(canonical)

    if not resolved_measures:
        inferred = _infer_measure_from_question(question)
        if inferred:
            resolved_measures = [inferred]

    intent["measures"] = resolved_measures

    # Aggregation
    intent["aggregation"] = _infer_aggregation(
        question,
        resolved_measures[0] if resolved_measures else None
    )

    return intent