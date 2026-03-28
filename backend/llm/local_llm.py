import httpx
import json

OLLAMA_URL = "http://localhost:11434"


def get_available_models():
    """
    Fetch locally available Ollama models
    """
    resp = httpx.get(f"{OLLAMA_URL}/api/tags", timeout=30)
    resp.raise_for_status()
    return [m["name"] for m in resp.json().get("models", [])]


def choose_best_llama_model(models: list[str]) -> str:
    """
    Choose the best available llama model in priority order
    """
    priority = [
        "llama3.1:8b",
        "llama3:latest",
        "llama3.2:3b",
        "llama3.2:1b",
    ]

    # Exact priority match
    for p in priority:
        if p in models:
            return p

    # Any other llama model
    for m in models:
        if m.lower().startswith("llama"):
            return m

    # Fallback to mistral
    for m in models:
        if "mistral" in m.lower():
            return m

    # Absolute fallback
    return models[0]


def _build_slim_schema(schema: dict) -> str:
    """
    Reduce schema to column names + types only.
    Prevents prompt from exceeding Ollama context window (num_ctx: 4096).
    Full schema with metadata/sample values can be 50k+ tokens.
    """
    slim = {}
    for col, info in schema.items():
        if isinstance(info, dict):
            slim[col] = info.get("dtype", "unknown")
        else:
            slim[col] = str(info)

    # Cap at 60 columns to keep prompt small
    keys = list(slim.keys())[:60]
    return json.dumps({k: slim[k] for k in keys}, indent=2)


def ask_llm(schema: dict, question: str, preferred_model: str | None = None) -> str:
    """
    Ask Ollama using the best available llama model.
    Uses a slimmed schema to avoid Ollama 500 errors from context overflow.
    """
    models = get_available_models()

    if not models:
        raise RuntimeError("No Ollama models found")

    # If user explicitly provided a model and it exists, use it
    if preferred_model and preferred_model in models:
        model = preferred_model
    else:
        model = choose_best_llama_model(models)

    slim_schema = _build_slim_schema(schema)

    prompt = f"""You are a senior FP&A analyst query parser.
Return ONLY a JSON object. No explanation, no markdown.

Available columns (name: type):
{slim_schema}

Question: {question}

Return exactly this structure:
{{
  "measures": ["<column_name>"],
  "filters": {{"<column_name>": "<value>"}},
  "aggregation": "sum"
}}

Rules:
- Only use column names from the schema above
- aggregation must be one of: sum, avg, count, min, max
- Do NOT calculate numbers
- Return JSON only, no extra text
"""

    try:
        with httpx.Client(timeout=httpx.Timeout(300.0)) as client:
            response = client.post(
                f"{OLLAMA_URL}/api/generate",
                json={
                    "model": model,
                    "prompt": prompt,
                    "stream": False,
                    "options": {
                        "temperature": 0.1,
                        "num_ctx": 4096
                    }
                }
            )

        response.raise_for_status()
        return response.json()["response"]

    except httpx.ReadTimeout:
        raise RuntimeError(
            f"Ollama timed out while using model '{model}'. "
            "Warm the model using: ollama run <model>"
        )