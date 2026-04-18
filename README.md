# Metis-Eduventures-Projects
End-to-end data analytics and KPI automation projects based on real-world business scenarios at Metis Eduventures, covering funnel optimization, cohort analysis, and scalable reporting systems.


# Project 1 
Adda247 Analytics — Dummy Project

> A fully self-contained dummy dataset + Athena/Presto-compatible SQL query library  
> that mirrors the real Adda247 analytics pipeline.  

---

## Project structure

```
adda_dummy/
├── README.md
├── sql/
│   └── kpi_queries.sql          ← All 5 KPI queries in one file
└── csv/
    ├── users_base_table.csv           ← 50,000 dummy users
    ├── purchase_success_374.csv       ← ~18,000 purchase transactions
    ├── start_session_adda_751.csv     ← ~105,000 live class sessions
    ├── video_engaged_164.csv          ← ~93,000 video engagement events
    ├── test_980.csv                   ← ~71,000 test attempts
    ├── content_consumed.csv           ← ~2M content consumption events
    ├── kpi_activation_daily.csv       ← KPI 1+2 pre-computed daily result
    ├── kpi_retention_cohort.csv       ← KPI 3 pre-computed monthly cohort result
    ├── kpi_signup_to_category_daily.csv ← KPI 4 pre-computed daily result
    ├── kpi_content_consumed_daily.csv ← KPI 5 pre-computed daily result
    └── kpi_summary_snapshot.csv       ← Single-row snapshot for KPI cards
```

---

## Data dictionary

### `users_base_table`
| Column | Type | Description |
|---|---|---|
| `user_id` | int | Unique user identifier |
| `signup_ts` | timestamp | Signup timestamp (IST) |
| `signup_date` | date | Signup date (IST) |
| `string_examcategory_selected_525` | string | Exam category chosen by user |
| `category_selected_date` | date | Date user selected exam category |
| `platform` | string | android / ios / web |
| `city` | string | User's city |

### `purchase_success_374`
| Column | Type | Description |
|---|---|---|
| `transaction_id` | string | Unique transaction ID |
| `user_id` | int | Foreign key → users |
| `server_time` | timestamp | Transaction time (UTC; add 330 min for IST) |
| `day` | date | Partition date (UTC) |
| `string_product_id_551` | string | Product purchased |
| `double_transaction_amount_257` | int | Transaction amount (INR) |
| `payment_mode` | string | upi / card / net_banking / wallet |
| `string_examcategory_selected_525` | string | Product's exam category |

### `start_session_adda_751` (Live class events)
| Column | Type | Description |
|---|---|---|
| `session_id` | string | Unique session ID |
| `user_id` | int | Foreign key → users |
| `server_time` | timestamp | Event time (UTC) |
| `day` | date | Partition date |
| `string_source_screen_172` | string | Screen from which session was started |
| `content_id` | string | Live class content ID |
| `string_examcategory_selected_525` | string | User's exam category |

### `video_engaged_164`
| Column | Type | Description |
|---|---|---|
| `engagement_id` | string | Unique engagement ID |
| `user_id` | int | Foreign key → users |
| `server_time` | timestamp | Event time (UTC) |
| `day` | date | Partition date |
| `video_id` | string | Video content ID |
| `watch_duration_seconds` | int | Duration watched in seconds |
| `string_examcategory_selected_525` | string | User's exam category |

### `test_980`
| Column | Type | Description |
|---|---|---|
| `test_attempt_id` | string | Unique attempt ID |
| `user_id` | int | Foreign key → users |
| `server_time` | timestamp | Event time (UTC) |
| `day` | date | Partition date |
| `string_content_title_878` | string | Test name |
| `string_user_action_105` | string | finished / started / abandoned |
| `score` | int | Score if finished (0–100) |
| `max_score` | int | Max possible score |
| `string_examcategory_selected_525` | string | User's exam category |

### `content_consumed`
| Column | Type | Description |
|---|---|---|
| `consumption_id` | string | Unique consumption ID |
| `user_id` | int | Foreign key → users |
| `server_time` | timestamp | Event time (UTC) |
| `day` | date | Partition date |
| `content_type` | string | live_class / recorded_video / test / notes / practice_quiz |
| `content_id` | string | Content item ID |
| `duration_seconds` | int | Time spent in seconds |
| `string_examcategory_selected_525` | string | User's exam category |

---

## KPI definitions & queries

All queries are in `sql/kpi_queries.sql`.  
Replace `dummy_db` with your actual schema name.  
Engine: **Athena / Presto / Open Analytics** (no `INTERVAL 'N' MONTH` — uses `date_add` throughout).

---

### KPI 1 — 24-hour Live Class + Video Activation

**Definition:** Percentage of users who purchased a paid product and then attended a live class OR watched a recorded video within 24 hours of their purchase date.

**Why it matters:** Early activation is the strongest predictor of long-term retention. A user who consumes content within 24 hours of purchase is significantly more likely to renew.

**Key design decisions in the query:**
- Live class (`start_session_adda_751`) and recorded video (`video_engaged_164`) are **merged** — a user counts once if they did either or both.
- `string_source_screen_172 NOT LIKE '%free%'` excludes free-tier sessions.
- The denominator (`ps_agg`) is pre-aggregated into scalar counts to avoid a CROSS JOIN explosion.
- W and W-1 use a **rolling 7-day average** (not ISO week-to-date) for fair comparison.

**Time windows returned:**

| Column | Window |
|---|---|
| `users_W_daily_avg` / `pct_W` | Rolling 7-day avg (D-6 → D-0) |
| `users_W1_daily_avg` / `pct_W1` | Prior rolling 7-day avg (D-13 → D-7) |
| `wow_delta_pct_pts` | W % minus W-1 % |
| `users_today` / `pct_today` | Yesterday (ref date) |
| `users_lw_same_day` / `pct_lw_same_day` | Same day last week |
| `users_MTD` / `pct_MTD` | Month to date |
| `users_LM_MTD` / `pct_LM_MTD` | Last month same period |
| `mom_mtd_delta_pct_pts` | MoM MTD delta in % points |
| `users_full_LM` / `pct_full_LM` | Full last month |
| `users_LY_WTD` / `pct_LY_WTD` | Same week last year |
| `users_LY_MTD` / `pct_LY_MTD` | Same MTD last year |
| `users_LY_LW` / `pct_LY_LW` | Last week prior year |
| `users_LY_LM` / `pct_LY_LM` | Last month prior year |
| `users_LY_full` / `pct_LY_full` | Full last year |

---

### KPI 2 — 24-hour Test Activation

**Definition:** Percentage of purchasers who completed a non-quiz test within 24 hours of their purchase date.

**Why it matters:** Test engagement is tracked separately from content consumption because it signals a higher intent level — a user actively testing knowledge, not just passively watching.

**Key design decisions:**
- Only `string_user_action_105 = 'finished'` counts (started/abandoned excluded).
- `lower(string_content_title_878) NOT LIKE '%quiz%'` removes short practice quizzes.
- Same 14-window time structure as KPI 1 for easy side-by-side comparison.

---

### KPI 3 — Retention (D7 and D30)

**Definition:** Percentage of purchasers who return and consume any content (live class, video, or test) on Day 7 (±1 day) and Day 30 (±1 day) after their purchase date.

**Why it matters:** Retention cohort analysis shows whether the product delivers long-term value. D7 indicates habit formation; D30 indicates sustained engagement and renewal likelihood.

**Key design decisions:**
- All content types are unioned into a single `all_activity` CTE before joining.
- A ±1 day window is used for D7 and D30 to account for timezone edge cases and irregular usage patterns.
- Results are grouped by `purchase_cohort_month` for cohort waterfall analysis in Power BI.

---

### KPI 4 — Signup to Category Selected

**Definition:** Percentage of new signups who select an exam category within D0 (same day), D1, D3, and D7 of signing up.

**Why it matters:** Category selection is the first personalisation step in the funnel. Users who select a category quickly are more likely to purchase. This metric helps measure onboarding funnel health.

**Key design decisions:**
- Four time thresholds (D0/D1/D3/D7) in a single query pass for efficiency.
- Category breakdown columns allow identification of which exam verticals are growing.
- Daily granularity enables day-of-week pattern analysis (weekends typically have higher D0 rates).

---

### KPI 5 — Content Consumed Per Day

**Definition:** Daily volume and depth of content consumption — total events, unique users (DAU), average items per user, and total/average watch time — broken down by content type and exam category.

**Why it matters:** Raw engagement volume. Feeds into cohort health, churn prediction, and content investment decisions. DAU trend is the primary leading indicator for platform growth.

**Key design decisions:**
- All five content types (`live_class`, `recorded_video`, `test`, `notes`, `practice_quiz`) tracked in parallel.
- `avg_items_per_user` and `avg_watch_min_per_user` normalise for DAU fluctuations.
- Category columns allow vertical-level health monitoring without a separate query.

---

## Power BI setup guide

**Recommended data model:**

```
users_base_table (1)
    ↓ user_id
purchase_success_374 (*)
    
purchase_success_374 (1) → ps_date
    ↓ date join
kpi_activation_daily (*)

users_base_table (1)
    ↓ signup_date
kpi_signup_to_category_daily (*)
```

**Suggested visuals per KPI:**

| KPI | Recommended visual |
|---|---|
| Activation | Line chart (daily %) + KPI card (W avg vs W-1 avg) |
| Activation by category | Clustered bar (pct by exam category) |
| Retention | Cohort table / waterfall (D7 vs D30 by month) |
| Signup → Category | Funnel chart (D0 → D1 → D3 → D7) + line trend |
| Content consumed | Area chart (DAU) + stacked bar (by content type) |
| Summary snapshot | KPI card grid with conditional formatting |

**Date table:** Create a calculated date dimension table in Power BI using:
```
DateTable = CALENDAR(DATE(2025,1,1), DATE(2025,4,14))
```
Add columns: Year, Month, Week, Day of Week, Is Weekend.

---

## Notes

- All timestamps in source tables are **UTC**. IST conversion = `date_add('minute', 330, server_time)`.
- Partition pruning: always filter on `date(day)` before applying the IST-converted date filter.
- The category `IN (...)` list in each query covers all exam verticals excluding school classes, CA, CUET, and other non-core verticals. Extend as needed.
- `NULLIF(..., 0)` is used throughout to prevent division-by-zero on sparse date windows.

- ## Dashboard Views

### KRA Overview Dashboard
![KPI Dashboard](screenshots/product_KRAs.png)

Tracks key business metrics like revenue, orders, AOV, and conversion rates.

---

### KPI Overview Dashboard
![Funnel Dashboard](screenshots/product_KPIs.png)

Shows user journey from installs to conversion, helping identify drop-offs.

