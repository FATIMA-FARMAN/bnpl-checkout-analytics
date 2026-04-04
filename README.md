# BNPL Checkout Analytics

**dbt + BigQuery analytics project** modelling the full checkout funnel, credit approval rates, and GMV for a Buy Now Pay Later platform.

Built to answer the questions a BNPL product team asks every day:
- Where are customers dropping out of the checkout flow?
- What is our credit approval rate, and why are applications being declined?
- How is GMV trending across merchants, countries, and installment plans?

---

## Data Architecture

```
Raw BigQuery tables
        │
   ┌────▼─────┐
   │ Staging  │  stg_checkout_sessions · stg_checkout_steps
   │  (views) │  stg_loan_applications · stg_orders · stg_merchants
   └────┬─────┘
        │
   ┌────▼──────────┐
   │ Intermediate  │  int_checkout_funnel · int_approval_rates
   │    (views)    │
   └────┬──────────┘
        │
   ┌────▼───────┐
   │   Marts    │  fct_checkout_funnel_daily
   │  (tables)  │  fct_approval_rates_daily
   │            │  fct_gmv_daily
   │            │  dim_merchants
   └────────────┘
```

---

## Key Models

| Model | Layer | Description |
|---|---|---|
| `stg_checkout_sessions` | Staging | One row per checkout session; normalises types and casing |
| `stg_checkout_steps` | Staging | Step-level events with time-on-step derived |
| `stg_loan_applications` | Staging | Credit applications with decision latency |
| `int_checkout_funnel` | Intermediate | Wide funnel table — one row per session with step-reached flags |
| `int_approval_rates` | Intermediate | Sessions joined to credit decisions |
| `fct_checkout_funnel_daily` | Mart | Daily funnel KPIs (conversion rate, step-over-step drop rates) |
| `fct_approval_rates_daily` | Mart | Daily approval/decline rates by merchant + installment plan |
| `fct_gmv_daily` | Mart | Daily GMV (realized vs gross), AOV, new vs returning split |
| `dim_merchants` | Mart | Merchant dimension with trailing 30-day performance metrics |

---

## Metrics Covered

**Funnel**
- Checkout conversion rate (sessions → confirmation)
- Step-over-step drop rates (cart → payment, details → credit check, etc.)
- Drop-off step identification per session

**Credit & Approval**
- Approval rate by merchant / country / installment plan
- Decline reason breakdown (insufficient history, high risk, amount exceeded, fraud)
- Average decision latency (seconds from application to decision)

**GMV & Revenue**
- Realized GMV vs gross GMV (net of cancellations/refunds)
- Average order value (AOV)
- New customer share of GMV

---

## Stack

| Tool | Purpose |
|---|---|
| dbt Core | Transformation layer, testing, documentation |
| BigQuery | Cloud data warehouse |
| dbt_utils | Cross-database macros |
| Looker / Looker Studio | Dashboard layer (uses marts directly) |

---

## Project Structure

```
models/
├── staging/          # Type-safe views over raw source tables
├── intermediate/     # Business logic joins (not exposed to BI tools)
└── marts/
    └── core/         # Production tables consumed by dashboards
analyses/             # Ad-hoc SQL (not materialized)
macros/               # Reusable Jinja macros
tests/                # Custom data tests
seeds/                # Reference / lookup tables
```

---

## Running the Project

```bash
# Install dependencies
dbt deps

# Run all models
dbt run

# Run tests
dbt test

# Run a specific model and its dependencies
dbt run --select +fct_checkout_funnel_daily

# Generate and serve documentation
dbt docs generate && dbt docs serve
```

---

## Contact

Fatima Farman — fatimafarman.fc@gmail.com · [LinkedIn](https://www.linkedin.com/in/fatima-farman)
