from engine.canonical_columns import CANONICAL_COLUMNS


def normalize(text: str) -> str:
    return text.strip().lower()


def resolve_to_canonical(column_name: str) -> str | None:
    col = normalize(column_name)

    for canonical, aliases in CANONICAL_COLUMNS.items():
        # Exact match on canonical name
        if col == canonical:
            return canonical
        # Exact match on any alias
        for a in aliases:
            if col == normalize(a):
                return canonical

    # Fuzzy fallback — partial containment for short abbreviations
    for canonical, aliases in CANONICAL_COLUMNS.items():
        for a in aliases:
            na = normalize(a)
            # If alias is short (<=4 chars), require exact match (already done above)
            # If longer, try containment
            if len(na) > 4 and (na in col or col in na):
                return canonical

    return None