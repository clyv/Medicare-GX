## Medicare GX Data Contracts

One data contract, enforced across **three execution environments** — Pandas, PostgreSQL and Spark — on the **CMS Medicare Physician & Other Practitioners 2023** dataset, using [Great Expectations 1.19.1](https://greatexpectations.io/).

> "I enforced a single data contract across Pandas, PostgreSQL and Spark on 11M rows of Medicare claims data — 96 rules that fail the build, published as an Open Data Contract Standard document."

📊 **[Browse the live Data Docs →](https://clyv.github.io/Medicare-GX/)** — regenerated on every run.

---

## What This Project Demonstrates

| Skill | Implementation |
|---|---|
| Data contracts as code | 115 rules across 7 quality dimensions and 81 columns |
| Open standard | Published as [ODCS](https://bitol.io/) v3.2 YAML, generated from the contract and drift-checked in CI |
| Tri-backend validation | The *same* suite runs on Pandas, PostgreSQL and Spark |
| Severity tiering | Blocking rules fail the build; advisory rules report without blocking |
| Custom expectations | NPI Luhn check digit (3 engines) and Benford's Law fraud screening |
| Cross-column integrity | Payment-chain and part-of-total rules a per-column check cannot see |
| Contract testing | pytest proves the contract catches the violations it claims to |
| CI/CD integration | GitHub Actions fails the build on broken data, publishes Data Docs to Pages |
| Real-world scale | CMS MUP 2023 — 1.26M provider rows, plus a 9.66M-row extract at a finer grain |

---

## Dataset

**CMS Medicare Physician & Other Practitioners — by Provider**
- Service year: 2023 (most recent public release, Dec 2025)
- Source: [data.cms.gov](https://data.cms.gov/provider-summary-by-type-of-service/medicare-physician-other-practitioners/medicare-physician-other-practitioners-by-provider)
- Size: ~1.26M rows × 81 columns — one row per rendering provider, aggregated across all services
- Contains: NPI, provider type, location, utilisation and payment totals, the drug/medical split of each total, beneficiary demographics, chronic condition prevalence, and HCC risk score

Two CMS privacy rules shape the contract, and both are asserted rather than worked around:

- **Suppression.** Any data element representing fewer than 11 beneficiaries is redacted. `*` marks a value suppressed for that reason, `#` one suppressed to stop the redacted figure being recomputed from its neighbours. Nulls in the demographic columns are therefore *expected*, so those columns are bounded by non-null proportion rather than demanded outright.
- **Top-coding.** Chronic condition percentages between 75 and 100 are top-coded at 75. The contract caps them at 75, not 100 — a value of 80 would mean the rule changed.

---

## Project Structure
```
Medicare-GX/
├── .github/workflows/
│   ├── data_quality.yml          # CI — contract tests, tri-backend validation, Pages
│   ├── data_quality_service.yml  # the 9.66M-row extract, monthly
│   └── schema_probe.yml          # on-demand header probe for a new extract
├── contracts/
│   └── mup_provider.odcs.yaml # published contract, generated from contract.py
├── data/raw/                  # downloaded CMS CSVs (gitignored)
├── docs/
│   └── gx-1.20-uniqueness-regression.md
├── gx/                        # GX context + suites (auto-generated, gitignored)
├── notebooks/
│   └── 01_exploration.ipynb   # EDA — schema profiling, null audit, contract derivation
├── pipelines/
│   ├── contract.py            # THE CONTRACT — pure data, no context, no I/O
│   ├── contract_service.py    # second contract, at the provider-service grain
│   ├── expectations/
│   │   ├── npi.py             # custom: NPI Luhn check digit
│   │   └── benford.py         # custom: Benford's Law screening
│   ├── validation.py          # tiered runner shared by all backends
│   ├── export_odcs.py         # contract.py -> ODCS YAML
│   ├── probe_schema.py        # read a remote CSV header before writing rules
│   ├── download_data.py
│   ├── build_suites.py
│   ├── load_to_postgres.py    # COPY FROM STDIN, explicit DDL
│   ├── validate_pandas.py
│   ├── validate_postgres.py
│   ├── validate_spark.py
│   └── validate_service.py    # the 9.66M-row extract, Postgres + Spark
├── tests/
│   └── test_expectations.py   # contract shape, behaviour, seeded violations
├── docker-compose.yml
├── requirements.txt
├── requirements-spark.txt
└── README.md
```

---

## The contract

`pipelines/contract.py` is the single source of truth. It is **pure data** — no GX context, no filesystem, no database — so it can be imported, diffed and unit tested without a 500MB download. `build_suites.py` persists it; the `validate_*` scripts run it.

Every rule carries a severity, a quality dimension and a rationale, and all three travel into Data Docs.

### Severity tiers

| Suite | Rules | Behaviour |
|---|---|---|
| `mup_provider_blocking` | 96 | Fails the build |
| `mup_provider_advisory` | 16 | Evaluated and published; never fails the build |
| `mup_provider_profiling` | 3 | Pandas-only screening; never fails the build |

**New rules land as advisory and are promoted only once a live run shows they hold**, so a threshold inferred from a data dictionary can never turn `main` red.

That promotion has now happened once, and the split it produced is the interesting part. The contract started with 27 blocking rules. After a tri-backend run agreed **85/85 on the advisory tier across Pandas, PostgreSQL and Spark**, everything *definitional* or drawn from *documented CMS methodology* was promoted — arithmetic identities, sign constraints, the 75 top-code, the suppression markers, the NPI check digit, the schema lock.

What stayed advisory is everything calibrated from a single year's measurements: proportion bounds, cardinality bounds, and two `mostly` tolerances. Those could legitimately shift with next year's release, and a contract should not block on a number nobody derived. A test enforces the distinction.

### Coverage

| Dimension | Rules |
|---|---|
| Schema | 12 required columns exist; the full 81-column set is locked, so an *added* column is caught too |
| Completeness | Five columns never null; demographic columns bounded by non-null proportion |
| Validity | NPI is 10 digits, matches `^\d{10}$`, and carries a correct Luhn check digit; state in the CMS code set; entity code `I`/`O`; suppression markers `*`/`#`; chronic percentages capped at the top code; risk score and average age in range; no negative money |
| Consistency | `Tot_Sbmtd_Chrg ≥ Tot_Mdcr_Alowd_Amt ≥ Tot_Mdcr_Pymt_Amt`, for totals and for both splits; no drug or medical part exceeds the total it belongs to; services ≥ beneficiaries |
| Uniqueness | One row per NPI |
| Volume | Row count between 1M and 15M |
| Distribution | Cardinality guards on state and provider type; Benford screening on the money columns |

**The cross-column rules are the ones worth pointing at.** Every per-column rule can pass on a row whose Medicare payment is ten times its own submitted charge. CMS defines the allowed amount as the payment plus deductible, coinsurance and third-party liability — so payment is a *component* of allowed and can never exceed it. That is an arithmetic invariant, and only a pair rule can see it.

### No type expectations, deliberately

`int64`/`float64` are Pandas dtype names, and `Rndrng_NPI` is `TEXT` in Postgres — loaded as a string so the 10-digit identifier survives intact. A type assertion could not pass on all three backends, and running one suite against all three is the point. Null, range, length and check-digit rules cover the same integrity concerns without the mismatch.

Column names keep their original CMS casing throughout. GX emits quoted SQL (`SELECT "Tot_Srvcs"`), which is case-sensitive in Postgres, so normalising the case would break every column lookup on the SQL side.

---

## Custom Expectations

### `expect_column_values_to_be_valid_npi`

The National Provider Identifier carries a check digit computed with the Luhn "double-add-double" formula (ISO/IEC 7812) over the identifier prefixed with **80840** — 80 for health applications, 840 for the United States.

`1234567890` is ten digits, non-null, unique and correctly formatted. It is still not a real NPI. No format rule can see that; only the check digit can.

Implemented for **Pandas, SQLAlchemy and Spark**, so it stays inside the portable contract. The SQL implementation guards its character-to-integer casts inside a `CASE`, because Postgres errors rather than returning null when casting non-numeric text.

### `expect_column_first_digits_to_follow_benfords_law`

Leading-digit screening using Nigrini's mean absolute deviation, with his published conformity bands. Benford screening of Medicare payment data is established in the literature ([Healthcare 2025, 13(12):1464](https://doi.org/10.3390/healthcare13121464)).

Pandas only — extracting a leading significant digit from an arbitrary float in portable SQL is not worth the complexity. Rather than quietly weaken the cross-backend guarantee, it lives in a third suite that only the Pandas run executes. It is a *screening signal*, not a contract term: fee schedules cluster around administered prices, which legitimately breaks the scale-invariance Benford assumes.

---

## A second dataset, at a different grain

`pipelines/contract_service.py` brings the **by-Provider-and-Service** extract under contract: ~10M rows, one row per provider per HCPCS code per place of service, against the by-provider file's 1.26M.

The interesting part is what changes with the grain:

- **Uniqueness becomes a compound key.** `Rndrng_NPI` alone is unique in the by-provider file. Copying that rule here would fail on correct data, because a provider legitimately appears once per procedure code per setting. The rule is `ExpectCompoundColumnsToBeUnique` over NPI + HCPCS + place of service — and there is a test asserting the single-column rule is *absent*, because that is the mistake copying the other contract would produce.
- **The suppression rule is different.** For the by-provider file CMS redacts small cells. For this one it drops the row: services covering 10 or fewer beneficiaries are excluded outright. So `Tot_Benes >= 11` is a property of how the file is published, not a plausibility guess.
- **Pandas is not offered.** 10M rows across 28 columns, one of them a long free-text HCPCS description, runs to several gigabytes before validation starts. Postgres and Spark are the engines built for it — which is the argument for a portable contract in the first place.

Its column names were read off the live file rather than inferred, using the schema probe:

```bash
python pipelines/probe_schema.py <csv-url> --rows 2
```

This runs on its own monthly schedule (`data_quality_service.yml`), not on push — a code change to the main contract does not need a 2GB download to re-run.

---

## Open Data Contract Standard

`contracts/mup_provider.odcs.yaml` publishes the contract in the Linux Foundation / [Bitol](https://bitol.io/) ODCS v3.2 format — readable by a catalogue, a consumer, or another team's tooling that has never heard of Great Expectations.

It is **generated** from `contract.py`, because a hand-written copy drifts. CI regenerates it and fails if the committed file differs, so the published contract and the executed contract cannot disagree. It is also linted against the standard with [`datacontract-cli`](https://cli.datacontract.com/).

The severity vocabularies line up exactly: an ODCS quality rule is `error` or `warning`, which is the same distinction as the blocking and advisory tiers.

```bash
python pipelines/export_odcs.py            # regenerate
python pipelines/export_odcs.py --check    # fail if stale
datacontract lint contracts/mup_provider.odcs.yaml
```

---

## Setup & Run

### 1. Clone & install
```bash
git clone https://github.com/clyv/Medicare-GX.git
cd Medicare-GX
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Start Postgres
```bash
docker-compose up -d
```

### 3. Create `.env`
```bash
POSTGRES_CONN=postgresql+psycopg2://gx_user:gx_password@localhost:5432/medicare_db
```

### 4. Run the tests (no download required)
```bash
pytest tests/ -v
```

### 5. Run the full pipeline
```bash
python pipelines/download_data.py       # ~500MB download
python pipelines/build_suites.py        # build the three suites
python pipelines/validate_pandas.py     # validate CSV -> builds Data Docs
python pipelines/load_to_postgres.py    # COPY into Postgres
python pipelines/validate_postgres.py   # validate SQL table
```

### 6. Spark (optional third backend)
```bash
pip install -r requirements-spark.txt   # needs a JVM (Java 17)
python pipelines/validate_spark.py
```

### 7. View Data Docs
```bash
open gx/uncommitted/data_docs/local_site/index.html          # macOS
xdg-open gx/uncommitted/data_docs/local_site/index.html      # Linux/WSL
start gx/uncommitted/data_docs/local_site/index.html         # Windows
```

---

## Testing

`tests/test_expectations.py` runs against synthetic frames carrying the full 81-column schema — no CMS download needed — and covers the contract from three sides:

- **Shape** — every rule declares severity, dimension and rationale; the tiers partition the contract; no backend-specific expectation slips in; the declared physical types partition the schema.
- **Behaviour** — a clean batch passes every tier, and 18 seeded violations each fail on the rule that should catch them. Four of those are only catchable by a cross-column rule: a payment exceeding its allowed amount, an allowed amount exceeding its submitted charge, a drug split exceeding its total, and more beneficiaries than services.
- **Integration** — suites survive a save-and-reload round trip (CI builds them in one process and validates in another, so a custom Expectation that cannot be resolved by type name on reload would break the pipeline), and the ODCS document matches the contract.

```bash
pytest tests/ -v
```

---

## CI/CD

Three jobs, so failures surface as early as they can:

**1. Contract tests & ODCS** — runs in about two minutes, before any data is fetched.
- Contract unit tests
- ODCS document is in step with `contract.py`
- ODCS document validates against the standard

**2. GX Validation** — the real thing.
- Spin up a Postgres 16 service container and a JVM
- Download and cache the CMS dataset
- Build the three suites
- Validate on Pandas, then load to Postgres with `COPY` and validate there, then validate on Spark
- **Fail the build** if any *blocking* rule is violated
- Upload Data Docs as an artifact

**3. Publish Data Docs** — deploys to [GitHub Pages](https://clyv.github.io/Medicare-GX/), including on failure, since a failing run is exactly the one worth reading.

The workflow also runs weekly, so upstream data drift surfaces without a push.

**Optional Slack alerting.** Set a `GX_SLACK_WEBHOOK_URL` repository secret and GX's `SlackNotificationAction` is attached to the blocking checkpoint. Only the blocking tier notifies — a channel that pings on every uncalibrated advisory threshold is a channel people mute. Without the secret, no action is attached and nothing changes.

---

## Tech Stack

- **Great Expectations 1.19.1** — data contracts engine
- **Pandas** — CSV backend
- **PostgreSQL 16** (Docker) — SQL backend, loaded with `COPY FROM STDIN`
- **Apache Spark 3.5** — distributed backend
- **ODCS v3.2** + `datacontract-cli` — published contract format
- **pytest** — contract tests
- **GitHub Actions** — CI/CD and Pages
- **Python 3.12**

> **Great Expectations is pinned to `1.19.1`.** Release 1.20.0 introduced `_build_duplicate_rows_source`, which regressed `expect_column_values_to_be_unique` on SQLAlchemy backends with mixed-case column names — `KeyError: 'Rndrng_NPI'`. The file is byte-identical in 1.21.0 and 1.22.0, so the bug is still live upstream. Reported upstream as [fivetran/great_expectations#12179](https://github.com/fivetran/great_expectations/issues/12179); reproduction and root-cause diff in [`docs/gx-1.20-uniqueness-regression.md`](docs/gx-1.20-uniqueness-regression.md).

---

## Final Run Order (local)

```bash
pytest tests/ -v
python pipelines/export_odcs.py --check
python pipelines/download_data.py
python pipelines/build_suites.py
python pipelines/validate_pandas.py
python pipelines/load_to_postgres.py
python pipelines/validate_postgres.py
python pipelines/validate_spark.py       # optional
```
