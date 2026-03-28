CANONICAL_COLUMNS = {
    "cycle": [
        "cycle", "period", "billing cycle", "billing period",
        "cyc", "cyl", "cycl", "cyce", "cyclee",
    ],
    "week_num": [
        "week #", "week", "week_num", "weeknum", "week number",
        "wk", "wk#", "wk num", "weekno", "week no",
    ],
    "file_num": [
        "file #", "file", "file_num", "filenum", "file number",
        "file no", "fileno",
    ],
    "worker_name": [
        "worker name", "worker", "employee", "employee name",
        "emp name", "staff", "staff name", "associate", "associate name",
        "contractor", "consultant", "resource", "talent",
        "worke", "employe", "assoicate", "assoc",
    ],
    "customer_name": [
        "customer name", "customer", "client", "client name",
        "account", "account name", "company", "organization",
        "org", "cust", "custmer", "compny", "clent",
    ],
    "location_code": [
        "location code", "location", "loc", "loc code", "site",
        "state", "region", "office", "locaton", "locaion",
    ],
    "join_date": [
        "join date", "start date", "hire date", "date joined",
        "onboard date", "start", "joining date", "joined",
    ],
    "end_date": [
        "end date", "termination date", "term date", "exit date",
        "offboard date", "end", "termination", "term",
    ],
    "pay_rate_reg": [
        "pay rate reg", "pay rate", "regular pay rate", "reg pay",
        "hourly pay", "pr", "pay", "payrate", "pay rt",
        "pay rate regular", "reg pay rate", "regular pay",
    ],
    "pay_rate_ot": [
        "pay rate ot", "overtime pay rate", "ot pay",
        "ot pay rate", "overtime pay",
    ],
    "pay_rate_dt": [
        "pay rate dt", "double time pay rate", "dt pay",
        "dt pay rate", "double time pay",
    ],
    "reg_hours": [
        "reg hours", "regular hours", "hours reg", "regular hrs",
        "std hours", "regular", "reg hrs", "normal hours",
    ],
    "ot_hours": [
        "ot hours", "overtime hours", "hours ot", "overtime hrs",
        "ot", "overtime",
    ],
    "dt_hours": [
        "dt hours", "double time hours", "hours dt",
        "dt", "double time",
    ],
    "bill_rate_reg": [
        "bill rate reg", "bill rate", "regular bill rate",
        "br", "br/hr", "billing rate", "bill rt", "billrate",
        "avg br", "avg br/hr", "average br", "average bill rate",
        "avg bill rate", "avg billing rate", "bill rate regular",
        "reg bill rate", "regular billing rate",
        "bilrate", "bil rate", "billrt",
    ],
    "bill_rate_ot": [
        "bill rate ot", "overtime bill rate", "ot bill rate",
        "ot billing rate",
    ],
    "bill_rate_dt": [
        "bill rate dt", "double time bill rate", "dt bill rate",
    ],
    "base_cost": [
        "base cost", "cost", "total cost", "expense",
        "base cst", "basecost", "loaded cost",
    ],
    "revenue": [
        "revenue", "total revenue", "sales", "gross revenue",
        "income", "total sales", "rev", "revenu", "reveneu",
        "reveue", "revnue", "revene", "revn", "reve",
        "total rev", "tot rev", "ttl revenue",
    ],
    # gm_dollars BEFORE gm_pct — "gross margin" without % means dollars
    "gm_dollars": [
        "gm$", "gm dollars", "gross margin $", "gross margin",
        "profit", "gm dollar", "total gm", "gm amount",
        "gross margin dollars", "gm $", "gmdollars",
        "total gross margin", "gross margin amount",
        "gm generated", "margin generated", "margin made",
    ],
    # gm_pct only when % or percent/pct explicitly in the question
    "gm_pct": [
        "gm%", "gm pct", "gross margin %", "margin %",
        "profit margin", "gm percent", "gm percentage",
        "gross margin percent", "gross margin percentage",
        "margin percent", "margin percentage",
        "gm %", "g.m%", "gm perc", "gmperc",
        "avg gm%", "avg gm pct", "average gm%",
    ],
    "total_hours": [
        "total hours", "hours", "hrs", "worked hours",
        "billable hours", "total hrs", "tot hours", "ttl hours",
        "hour", "hr", "no of hours", "number of hours",
        "hours worked",
    ],
    "month": ["month", "mth", "mo", "mnth"],
    "quarter": [
        "quarter", "qtr", "q",
        "q1", "q2", "q3", "q4",
        "quarter 1", "quarter 2", "quarter 3", "quarter 4",
        "first quarter", "second quarter", "third quarter", "fourth quarter",
        "qurter", "quartr",
    ],
    "year": ["year", "yr", "fiscal year", "fy", "annual"],
    "headcount": [
        "headcount", "head count", "hc", "no of workers",
        "number of workers", "no of associates", "number of associates",
        "no of employees", "number of employees", "no of people",
        "worker count", "employee count", "associate count",
        "how many workers", "how many associates", "how many employees",
        "how many people", "staff count", "total workers",
        "total associates", "total employees", "total headcount",
        "associates", "workers", "employees",
    ],
}