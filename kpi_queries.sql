-- ============================================================
-- ADDA247 DUMMY PROJECT — KPI QUERIES
-- Engine  : Athena / Presto (Open Analytics compatible)
-- Schema  : dummy_db  (replace with your actual schema)
-- Dates   : 2025-01-01 onwards  |  ref_date = yesterday
-- Author  : Analytics Team
-- ============================================================

-- ────────────────────────────────────────────────────────────
-- SHARED CATEGORY FILTER  (paste inside any ps CTE)
-- Maps all raw exam category strings → clean buckets
-- ────────────────────────────────────────────────────────────
/*
  b.string_examcategory_selected_525 IN (
    'BANKING','SSC','RAILWAYS','DEFENCE','CTET','UPSC',
    'ENGINEERING','GATE','IIT JEE','NEET',
    'UTTAR PRADESH','BIHAR','RAJASTHAN','MAHARASHTRA',
    'MADHYA PRADESH','HARYANA','GUJARAT','PUNJAB_STATE_EXAMS',
    'TAMIL_NADU','KERALA','WEST_BENGAL'
  )
*/


-- ============================================================
-- KPI 1 ── 24-HOUR LIVE CLASS ACTIVATION
-- Definition : Purchased users who attended a live class
--              within 24 hours of their purchase date.
-- Output     : User count + % of purchasers, across all
--              standard time windows (W, W-1, Today, MTD …)
-- ============================================================

WITH

ps AS (
  SELECT
    a.user_id,
    date(date_add('minute', 330, a.server_time))  AS ps_date
  FROM dummy_db.purchase_success_374 AS a
  JOIN dummy_db.users_base_table     AS b ON a.user_id = b.user_id
  WHERE date(a.day)                                         >= date('2025-01-01')
    AND date(date_add('minute', 330, a.server_time))        >= date('2025-01-01')
    AND a.double_transaction_amount_257                      >  0
    AND b.string_examcategory_selected_525 IN (
        'BANKING','SSC','RAILWAYS','DEFENCE','CTET','UPSC',
        'ENGINEERING','GATE','IIT JEE','NEET',
        'UTTAR PRADESH','BIHAR','RAJASTHAN','MAHARASHTRA',
        'MADHYA PRADESH','HARYANA','GUJARAT','PUNJAB_STATE_EXAMS',
        'TAMIL_NADU','KERALA','WEST_BENGAL'
    )
),

lc AS (
  SELECT
    user_id,
    date(date_add('minute', 330, a.server_time))  AS lc_date
  FROM dummy_db.start_session_adda_751 AS a
  WHERE date(a.day)                                         >= date('2025-01-01')
    AND date(date_add('minute', 330, a.server_time))        >= date('2025-01-01')
    AND lower(string_source_screen_172) NOT LIKE '%free%'
),

vc AS (
  SELECT
    user_id,
    date(date_add('minute', 330, a.server_time))  AS vc_date
  FROM dummy_db.video_engaged_164 AS a
  WHERE date(a.day)                                         >= date('2025-01-01')
    AND date(date_add('minute', 330, a.server_time))        >= date('2025-01-01')
),

-- Merge live class + video into a single "content activation" event
lv_engagement AS (
  SELECT DISTINCT
    ps.user_id,
    ps.ps_date AS purchase_date
  FROM ps
  LEFT JOIN lc
         ON ps.user_id = lc.user_id
        AND lc.lc_date BETWEEN ps.ps_date AND date_add('day', 1, ps.ps_date)
  LEFT JOIN vc
         ON ps.user_id = vc.user_id
        AND vc.vc_date BETWEEN ps.ps_date AND date_add('day', 1, ps.ps_date)
  WHERE lc.user_id IS NOT NULL
     OR vc.user_id IS NOT NULL
),

-- Pre-aggregate denominators (total purchasers per window) → avoids CROSS JOIN explosion
ps_agg AS (
  SELECT
    date_add('day', -1, current_date)                                                          AS today,
    date_trunc('week',  date_add('day', -1, current_date))                                     AS w_start,
    date_add('day',  -6, date_add('day', -1, current_date))                                    AS r7_start,
    date_add('day', -13, date_add('day', -1, current_date))                                    AS r7_prev_start,
    date_add('day',  -7, date_add('day', -1, current_date))                                    AS r7_prev_end,
    date_trunc('month', date_add('day', -1, current_date))                                     AS mtd_start,
    date_add('month', -1, date_trunc('month', date_add('day', -1, current_date)))              AS lm_start,
    date_add('month', -1, date_add('day', -1, current_date))                                   AS lm_till_day,
    date_add('day',   -1, date_trunc('month', date_add('day', -1, current_date)))              AS lm_end,
    date_add('week', -52, date_trunc('week', date_add('day', -1, current_date)))               AS ly_w_start,
    date_add('week', -52, date_add('day', -1, current_date))                                   AS ly_w_end,
    date_add('year',  -1, date_trunc('month', date_add('day', -1, current_date)))              AS ly_m_start,
    date_add('year',  -1, date_add('day', -1, current_date))                                   AS ly_m_end,
    date_add('day',  -7, date_add('week', -52, date_trunc('week', date_add('day',-1,current_date)))) AS ly_lw_start,
    date_add('day',  -1, date_add('week', -52, date_trunc('week', date_add('day',-1,current_date)))) AS ly_lw_end,
    date_add('month',-1, date_add('year', -1, date_trunc('month', date_add('day',-1,current_date)))) AS ly_lm_start,
    date_add('day',  -1, date_add('year', -1, date_trunc('month', date_add('day',-1,current_date)))) AS ly_lm_end,
    date_add('year', -1, date_trunc('year',  date_add('day', -1, current_date)))               AS ly_fy_start,
    date_add('day',  -1, date_trunc('year',  date_add('day', -1, current_date)))               AS ly_fy_end,

    -- Rolling 7-day denominators
    count(distinct case when ps_date between date_add('day',-6,date_add('day',-1,current_date))
      and date_add('day',-1,current_date) then user_id end)                                    AS denom_W,
    count(distinct case when ps_date between date_add('day',-13,date_add('day',-1,current_date))
      and date_add('day',-7,date_add('day',-1,current_date)) then user_id end)                 AS denom_W1,
    count(distinct case when ps_date = date_add('day',-1,current_date) then user_id end)       AS denom_today,
    count(distinct case when ps_date = date_add('day',-8,current_date) then user_id end)       AS denom_lw_same_day,
    count(distinct case when ps_date between date_trunc('month',date_add('day',-1,current_date))
      and date_add('day',-1,current_date) then user_id end)                                    AS denom_MTD,
    count(distinct case when ps_date between date_add('month',-1,date_trunc('month',date_add('day',-1,current_date)))
      and date_add('month',-1,date_add('day',-1,current_date)) then user_id end)               AS denom_LM_MTD,
    count(distinct case when ps_date between date_add('month',-1,date_trunc('month',date_add('day',-1,current_date)))
      and date_add('day',-1,date_trunc('month',date_add('day',-1,current_date))) then user_id end) AS denom_full_LM,
    count(distinct case when ps_date between date_add('week',-52,date_trunc('week',date_add('day',-1,current_date)))
      and date_add('week',-52,date_add('day',-1,current_date)) then user_id end)               AS denom_LY_WTD,
    count(distinct case when ps_date between date_add('year',-1,date_trunc('month',date_add('day',-1,current_date)))
      and date_add('year',-1,date_add('day',-1,current_date)) then user_id end)                AS denom_LY_MTD,
    count(distinct case when ps_date between
      date_add('day',-7,date_add('week',-52,date_trunc('week',date_add('day',-1,current_date))))
      and date_add('day',-1,date_add('week',-52,date_trunc('week',date_add('day',-1,current_date))))
      then user_id end)                                                                         AS denom_LY_LW,
    count(distinct case when ps_date between
      date_add('month',-1,date_add('year',-1,date_trunc('month',date_add('day',-1,current_date))))
      and date_add('day',-1,date_add('year',-1,date_trunc('month',date_add('day',-1,current_date))))
      then user_id end)                                                                         AS denom_LY_LM,
    count(distinct case when ps_date between
      date_add('year',-1,date_trunc('year',date_add('day',-1,current_date)))
      and date_add('day',-1,date_trunc('year',date_add('day',-1,current_date)))
      then user_id end)                                                                         AS denom_LY_full

  FROM (SELECT DISTINCT user_id, ps_date FROM ps) ps_dedup
)

SELECT
  'Live + Video Activation (24hr)' AS kpi,

  -- 1. Rolling 7-day avg (W)
  round(count(distinct case when e.purchase_date between d.r7_start and d.today then e.user_id end) / 7.0, 1) AS users_W_daily_avg,
  round(100.0 * (count(distinct case when e.purchase_date between d.r7_start and d.today then e.user_id end) / 7.0) / nullif(d.denom_W / 7.0, 0), 2) AS pct_W,

  -- 2. Rolling 7-day avg prior week (W-1)
  round(count(distinct case when e.purchase_date between d.r7_prev_start and d.r7_prev_end then e.user_id end) / 7.0, 1) AS users_W1_daily_avg,
  round(100.0 * (count(distinct case when e.purchase_date between d.r7_prev_start and d.r7_prev_end then e.user_id end) / 7.0) / nullif(d.denom_W1 / 7.0, 0), 2) AS pct_W1,

  -- 3. Today (yesterday as ref)
  count(distinct case when e.purchase_date = d.today then e.user_id end) AS users_today,
  round(100.0 * count(distinct case when e.purchase_date = d.today then e.user_id end) / nullif(d.denom_today, 0), 2) AS pct_today,

  -- 4. Last week same day
  count(distinct case when e.purchase_date = d.r7_prev_end then e.user_id end) AS users_lw_same_day,
  round(100.0 * count(distinct case when e.purchase_date = d.r7_prev_end then e.user_id end) / nullif(d.denom_lw_same_day, 0), 2) AS pct_lw_same_day,

  -- 5. WoW Δ (% points)
  round(
    100.0 * (count(distinct case when e.purchase_date between d.r7_start     and d.today        then e.user_id end) / 7.0) / nullif(d.denom_W  / 7.0, 0)
  - 100.0 * (count(distinct case when e.purchase_date between d.r7_prev_start and d.r7_prev_end then e.user_id end) / 7.0) / nullif(d.denom_W1 / 7.0, 0)
  , 2) AS wow_delta_pct_pts,

  -- 6. MTD
  count(distinct case when e.purchase_date between d.mtd_start and d.today then e.user_id end) AS users_MTD,
  round(100.0 * count(distinct case when e.purchase_date between d.mtd_start and d.today then e.user_id end) / nullif(d.denom_MTD, 0), 2) AS pct_MTD,

  -- 7. Last month MTD
  count(distinct case when e.purchase_date between d.lm_start and d.lm_till_day then e.user_id end) AS users_LM_MTD,
  round(100.0 * count(distinct case when e.purchase_date between d.lm_start and d.lm_till_day then e.user_id end) / nullif(d.denom_LM_MTD, 0), 2) AS pct_LM_MTD,

  -- 8. MoM MTD Δ
  round(
    100.0 * count(distinct case when e.purchase_date between d.mtd_start and d.today        then e.user_id end) / nullif(d.denom_MTD,    0)
  - 100.0 * count(distinct case when e.purchase_date between d.lm_start  and d.lm_till_day then e.user_id end) / nullif(d.denom_LM_MTD, 0)
  , 2) AS mom_mtd_delta_pct_pts,

  -- 9. Full last month
  count(distinct case when e.purchase_date between d.lm_start and d.lm_end then e.user_id end) AS users_full_LM,
  round(100.0 * count(distinct case when e.purchase_date between d.lm_start and d.lm_end then e.user_id end) / nullif(d.denom_full_LM, 0), 2) AS pct_full_LM,

  -- 10. Same week WTD LY
  count(distinct case when e.purchase_date between d.ly_w_start and d.ly_w_end then e.user_id end) AS users_LY_WTD,
  round(100.0 * count(distinct case when e.purchase_date between d.ly_w_start and d.ly_w_end then e.user_id end) / nullif(d.denom_LY_WTD, 0), 2) AS pct_LY_WTD,

  -- 11. Same MTD LY
  count(distinct case when e.purchase_date between d.ly_m_start and d.ly_m_end then e.user_id end) AS users_LY_MTD,
  round(100.0 * count(distinct case when e.purchase_date between d.ly_m_start and d.ly_m_end then e.user_id end) / nullif(d.denom_LY_MTD, 0), 2) AS pct_LY_MTD,

  -- 12. Last week previous year
  count(distinct case when e.purchase_date between d.ly_lw_start and d.ly_lw_end then e.user_id end) AS users_LY_LW,
  round(100.0 * count(distinct case when e.purchase_date between d.ly_lw_start and d.ly_lw_end then e.user_id end) / nullif(d.denom_LY_LW, 0), 2) AS pct_LY_LW,

  -- 13. Last month previous year
  count(distinct case when e.purchase_date between d.ly_lm_start and d.ly_lm_end then e.user_id end) AS users_LY_LM,
  round(100.0 * count(distinct case when e.purchase_date between d.ly_lm_start and d.ly_lm_end then e.user_id end) / nullif(d.denom_LY_LM, 0), 2) AS pct_LY_LM,

  -- 14. Last year full
  count(distinct case when e.purchase_date between d.ly_fy_start and d.ly_fy_end then e.user_id end) AS users_LY_full,
  round(100.0 * count(distinct case when e.purchase_date between d.ly_fy_start and d.ly_fy_end then e.user_id end) / nullif(d.denom_LY_full, 0), 2) AS pct_LY_full

FROM lv_engagement e
CROSS JOIN ps_agg d
GROUP BY
  d.today, d.r7_start, d.r7_prev_start, d.r7_prev_end,
  d.mtd_start, d.lm_start, d.lm_till_day, d.lm_end,
  d.ly_w_start, d.ly_w_end, d.ly_m_start, d.ly_m_end,
  d.ly_lw_start, d.ly_lw_end, d.ly_lm_start, d.ly_lm_end,
  d.ly_fy_start, d.ly_fy_end,
  d.denom_W, d.denom_W1, d.denom_today, d.denom_lw_same_day,
  d.denom_MTD, d.denom_LM_MTD, d.denom_full_LM,
  d.denom_LY_WTD, d.denom_LY_MTD, d.denom_LY_LW, d.denom_LY_LM, d.denom_LY_full;


-- ============================================================
-- KPI 2 ── 24-HOUR TEST ACTIVATION
-- Definition : Purchased users who finished a non-quiz test
--              within 24 hours of their purchase date.
-- ============================================================

WITH

ps AS (
  SELECT
    a.user_id,
    date(date_add('minute', 330, a.server_time))  AS ps_date
  FROM dummy_db.purchase_success_374 AS a
  JOIN dummy_db.users_base_table     AS b ON a.user_id = b.user_id
  WHERE date(a.day)                                         >= date('2025-01-01')
    AND date(date_add('minute', 330, a.server_time))        >= date('2025-01-01')
    AND a.double_transaction_amount_257                      >  0
    AND b.string_examcategory_selected_525 IN (
        'BANKING','SSC','RAILWAYS','DEFENCE','CTET','UPSC',
        'ENGINEERING','GATE','IIT JEE','NEET',
        'UTTAR PRADESH','BIHAR','RAJASTHAN','MAHARASHTRA',
        'MADHYA PRADESH','HARYANA','GUJARAT','PUNJAB_STATE_EXAMS',
        'TAMIL_NADU','KERALA','WEST_BENGAL'
    )
),

test AS (
  SELECT
    user_id,
    date(date_add('minute', 330, a.server_time))  AS test_date
  FROM dummy_db.test_980 AS a
  WHERE date(a.day)                                         >= date('2025-01-01')
    AND date(date_add('minute', 330, a.server_time))        >= date('2025-01-01')
    AND string_user_action_105                               = 'finished'
    AND lower(string_content_title_878)                     NOT LIKE '%quiz%'
),

test_engagement AS (
  SELECT DISTINCT
    ps.user_id,
    ps.ps_date AS purchase_date
  FROM ps
  INNER JOIN test t
          ON ps.user_id  = t.user_id
         AND t.test_date BETWEEN ps.ps_date AND date_add('day', 1, ps.ps_date)
),

ps_agg AS (
  SELECT
    date_add('day', -1, current_date)                                                          AS today,
    date_add('day',  -6, date_add('day', -1, current_date))                                    AS r7_start,
    date_add('day', -13, date_add('day', -1, current_date))                                    AS r7_prev_start,
    date_add('day',  -7, date_add('day', -1, current_date))                                    AS r7_prev_end,
    date_trunc('month', date_add('day', -1, current_date))                                     AS mtd_start,
    date_add('month', -1, date_trunc('month', date_add('day', -1, current_date)))              AS lm_start,
    date_add('month', -1, date_add('day', -1, current_date))                                   AS lm_till_day,
    date_add('day',   -1, date_trunc('month', date_add('day', -1, current_date)))              AS lm_end,
    date_add('week', -52, date_trunc('week', date_add('day', -1, current_date)))               AS ly_w_start,
    date_add('week', -52, date_add('day', -1, current_date))                                   AS ly_w_end,
    date_add('year',  -1, date_trunc('month', date_add('day', -1, current_date)))              AS ly_m_start,
    date_add('year',  -1, date_add('day', -1, current_date))                                   AS ly_m_end,
    date_add('day',  -7, date_add('week',-52, date_trunc('week',date_add('day',-1,current_date)))) AS ly_lw_start,
    date_add('day',  -1, date_add('week',-52, date_trunc('week',date_add('day',-1,current_date)))) AS ly_lw_end,
    date_add('month',-1, date_add('year',-1,  date_trunc('month',date_add('day',-1,current_date)))) AS ly_lm_start,
    date_add('day',  -1, date_add('year',-1,  date_trunc('month',date_add('day',-1,current_date)))) AS ly_lm_end,
    date_add('year', -1, date_trunc('year',   date_add('day', -1, current_date)))              AS ly_fy_start,
    date_add('day',  -1, date_trunc('year',   date_add('day', -1, current_date)))              AS ly_fy_end,

    count(distinct case when ps_date between date_add('day',-6,date_add('day',-1,current_date))
      and date_add('day',-1,current_date) then user_id end)                                    AS denom_W,
    count(distinct case when ps_date between date_add('day',-13,date_add('day',-1,current_date))
      and date_add('day',-7,date_add('day',-1,current_date)) then user_id end)                 AS denom_W1,
    count(distinct case when ps_date = date_add('day',-1,current_date) then user_id end)       AS denom_today,
    count(distinct case when ps_date = date_add('day',-8,current_date) then user_id end)       AS denom_lw_same_day,
    count(distinct case when ps_date between date_trunc('month',date_add('day',-1,current_date))
      and date_add('day',-1,current_date) then user_id end)                                    AS denom_MTD,
    count(distinct case when ps_date between date_add('month',-1,date_trunc('month',date_add('day',-1,current_date)))
      and date_add('month',-1,date_add('day',-1,current_date)) then user_id end)               AS denom_LM_MTD,
    count(distinct case when ps_date between date_add('month',-1,date_trunc('month',date_add('day',-1,current_date)))
      and date_add('day',-1,date_trunc('month',date_add('day',-1,current_date))) then user_id end) AS denom_full_LM,
    count(distinct case when ps_date between date_add('week',-52,date_trunc('week',date_add('day',-1,current_date)))
      and date_add('week',-52,date_add('day',-1,current_date)) then user_id end)               AS denom_LY_WTD,
    count(distinct case when ps_date between date_add('year',-1,date_trunc('month',date_add('day',-1,current_date)))
      and date_add('year',-1,date_add('day',-1,current_date)) then user_id end)                AS denom_LY_MTD,
    count(distinct case when ps_date between
      date_add('day',-7,date_add('week',-52,date_trunc('week',date_add('day',-1,current_date))))
      and date_add('day',-1,date_add('week',-52,date_trunc('week',date_add('day',-1,current_date))))
      then user_id end)                                                                         AS denom_LY_LW,
    count(distinct case when ps_date between
      date_add('month',-1,date_add('year',-1,date_trunc('month',date_add('day',-1,current_date))))
      and date_add('day',-1,date_add('year',-1,date_trunc('month',date_add('day',-1,current_date))))
      then user_id end)                                                                         AS denom_LY_LM,
    count(distinct case when ps_date between
      date_add('year',-1,date_trunc('year',date_add('day',-1,current_date)))
      and date_add('day',-1,date_trunc('year',date_add('day',-1,current_date)))
      then user_id end)                                                                         AS denom_LY_full

  FROM (SELECT DISTINCT user_id, ps_date FROM ps) ps_dedup
)

SELECT
  'Test Activation (24hr)' AS kpi,
  round(count(distinct case when e.purchase_date between d.r7_start     and d.today        then e.user_id end) / 7.0, 1) AS users_W_daily_avg,
  round(100.0*(count(distinct case when e.purchase_date between d.r7_start     and d.today        then e.user_id end)/7.0)/nullif(d.denom_W/7.0,0),2) AS pct_W,
  round(count(distinct case when e.purchase_date between d.r7_prev_start and d.r7_prev_end then e.user_id end) / 7.0, 1) AS users_W1_daily_avg,
  round(100.0*(count(distinct case when e.purchase_date between d.r7_prev_start and d.r7_prev_end then e.user_id end)/7.0)/nullif(d.denom_W1/7.0,0),2) AS pct_W1,
  count(distinct case when e.purchase_date = d.today        then e.user_id end) AS users_today,
  round(100.0*count(distinct case when e.purchase_date = d.today        then e.user_id end)/nullif(d.denom_today,0),2) AS pct_today,
  count(distinct case when e.purchase_date = d.r7_prev_end  then e.user_id end) AS users_lw_same_day,
  round(100.0*count(distinct case when e.purchase_date = d.r7_prev_end  then e.user_id end)/nullif(d.denom_lw_same_day,0),2) AS pct_lw_same_day,
  round(100.0*(count(distinct case when e.purchase_date between d.r7_start and d.today then e.user_id end)/7.0)/nullif(d.denom_W/7.0,0)
       -100.0*(count(distinct case when e.purchase_date between d.r7_prev_start and d.r7_prev_end then e.user_id end)/7.0)/nullif(d.denom_W1/7.0,0),2) AS wow_delta_pct_pts,
  count(distinct case when e.purchase_date between d.mtd_start and d.today        then e.user_id end) AS users_MTD,
  round(100.0*count(distinct case when e.purchase_date between d.mtd_start and d.today        then e.user_id end)/nullif(d.denom_MTD,0),2) AS pct_MTD,
  count(distinct case when e.purchase_date between d.lm_start  and d.lm_till_day  then e.user_id end) AS users_LM_MTD,
  round(100.0*count(distinct case when e.purchase_date between d.lm_start  and d.lm_till_day  then e.user_id end)/nullif(d.denom_LM_MTD,0),2) AS pct_LM_MTD,
  round(100.0*count(distinct case when e.purchase_date between d.mtd_start and d.today        then e.user_id end)/nullif(d.denom_MTD,0)
       -100.0*count(distinct case when e.purchase_date between d.lm_start  and d.lm_till_day  then e.user_id end)/nullif(d.denom_LM_MTD,0),2) AS mom_mtd_delta_pct_pts,
  count(distinct case when e.purchase_date between d.lm_start  and d.lm_end       then e.user_id end) AS users_full_LM,
  round(100.0*count(distinct case when e.purchase_date between d.lm_start  and d.lm_end       then e.user_id end)/nullif(d.denom_full_LM,0),2) AS pct_full_LM,
  count(distinct case when e.purchase_date between d.ly_w_start and d.ly_w_end    then e.user_id end) AS users_LY_WTD,
  round(100.0*count(distinct case when e.purchase_date between d.ly_w_start and d.ly_w_end    then e.user_id end)/nullif(d.denom_LY_WTD,0),2) AS pct_LY_WTD,
  count(distinct case when e.purchase_date between d.ly_m_start and d.ly_m_end    then e.user_id end) AS users_LY_MTD,
  round(100.0*count(distinct case when e.purchase_date between d.ly_m_start and d.ly_m_end    then e.user_id end)/nullif(d.denom_LY_MTD,0),2) AS pct_LY_MTD,
  count(distinct case when e.purchase_date between d.ly_lw_start and d.ly_lw_end  then e.user_id end) AS users_LY_LW,
  round(100.0*count(distinct case when e.purchase_date between d.ly_lw_start and d.ly_lw_end  then e.user_id end)/nullif(d.denom_LY_LW,0),2) AS pct_LY_LW,
  count(distinct case when e.purchase_date between d.ly_lm_start and d.ly_lm_end  then e.user_id end) AS users_LY_LM,
  round(100.0*count(distinct case when e.purchase_date between d.ly_lm_start and d.ly_lm_end  then e.user_id end)/nullif(d.denom_LY_LM,0),2) AS pct_LY_LM,
  count(distinct case when e.purchase_date between d.ly_fy_start and d.ly_fy_end  then e.user_id end) AS users_LY_full,
  round(100.0*count(distinct case when e.purchase_date between d.ly_fy_start and d.ly_fy_end  then e.user_id end)/nullif(d.denom_LY_full,0),2) AS pct_LY_full

FROM test_engagement e
CROSS JOIN ps_agg d
GROUP BY
  d.today, d.r7_start, d.r7_prev_start, d.r7_prev_end,
  d.mtd_start, d.lm_start, d.lm_till_day, d.lm_end,
  d.ly_w_start, d.ly_w_end, d.ly_m_start, d.ly_m_end,
  d.ly_lw_start, d.ly_lw_end, d.ly_lm_start, d.ly_lm_end,
  d.ly_fy_start, d.ly_fy_end,
  d.denom_W, d.denom_W1, d.denom_today, d.denom_lw_same_day,
  d.denom_MTD, d.denom_LM_MTD, d.denom_full_LM,
  d.denom_LY_WTD, d.denom_LY_MTD, d.denom_LY_LW, d.denom_LY_LM, d.denom_LY_full;


-- ============================================================
-- KPI 3 ── RETENTION
-- Definition : Purchased users who return and consume ANY
--              content (live, video, or test) on Day 7 and
--              Day 30 after their purchase date.
-- ============================================================

WITH

ps AS (
  SELECT DISTINCT
    a.user_id,
    date(date_add('minute', 330, a.server_time)) AS ps_date
  FROM dummy_db.purchase_success_374 AS a
  JOIN dummy_db.users_base_table     AS b ON a.user_id = b.user_id
  WHERE date(a.day)                                         >= date('2025-01-01')
    AND date(date_add('minute', 330, a.server_time))        >= date('2025-01-01')
    AND a.double_transaction_amount_257                      >  0
    AND b.string_examcategory_selected_525 IN (
        'BANKING','SSC','RAILWAYS','DEFENCE','CTET','UPSC',
        'ENGINEERING','GATE','IIT JEE','NEET',
        'UTTAR PRADESH','BIHAR','RAJASTHAN','MAHARASHTRA',
        'MADHYA PRADESH','HARYANA','GUJARAT','PUNJAB_STATE_EXAMS',
        'TAMIL_NADU','KERALA','WEST_BENGAL'
    )
),

-- Union all content events into one activity table
all_activity AS (
  SELECT user_id, date(date_add('minute', 330, server_time)) AS activity_date
  FROM dummy_db.start_session_adda_751
  WHERE date(day) >= date('2025-01-01')
    AND lower(string_source_screen_172) NOT LIKE '%free%'
  UNION ALL
  SELECT user_id, date(date_add('minute', 330, server_time)) AS activity_date
  FROM dummy_db.video_engaged_164
  WHERE date(day) >= date('2025-01-01')
  UNION ALL
  SELECT user_id, date(date_add('minute', 330, server_time)) AS activity_date
  FROM dummy_db.test_980
  WHERE date(day) >= date('2025-01-01')
    AND string_user_action_105 = 'finished'
    AND lower(string_content_title_878) NOT LIKE '%quiz%'
),

retention_base AS (
  SELECT
    ps.user_id,
    ps.ps_date,
    -- Day 7 retention flag
    max(case when a7.activity_date
             between date_add('day', 6,  ps.ps_date)
             and     date_add('day', 8,  ps.ps_date)  -- ±1 day window
             then 1 else 0 end) AS retained_d7,
    -- Day 30 retention flag
    max(case when a30.activity_date
             between date_add('day', 29, ps.ps_date)
             and     date_add('day', 31, ps.ps_date)
             then 1 else 0 end) AS retained_d30
  FROM ps
  LEFT JOIN all_activity a7
         ON ps.user_id = a7.user_id
        AND a7.activity_date BETWEEN date_add('day', 6, ps.ps_date) AND date_add('day', 8, ps.ps_date)
  LEFT JOIN all_activity a30
         ON ps.user_id = a30.user_id
        AND a30.activity_date BETWEEN date_add('day', 29, ps.ps_date) AND date_add('day', 31, ps.ps_date)
  GROUP BY ps.user_id, ps.ps_date
)

SELECT
  'Retention'                                                                       AS kpi,
  -- D7 retention
  sum(retained_d7)                                                                  AS d7_retained_users,
  count(distinct user_id)                                                           AS total_purchase_users,
  round(100.0 * sum(retained_d7)  / nullif(count(distinct user_id), 0), 2)         AS d7_retention_pct,
  -- D30 retention
  sum(retained_d30)                                                                 AS d30_retained_users,
  round(100.0 * sum(retained_d30) / nullif(count(distinct user_id), 0), 2)         AS d30_retention_pct,
  -- by purchase month cohort
  date_trunc('month', ps_date)                                                      AS purchase_cohort_month

FROM retention_base
GROUP BY date_trunc('month', ps_date)
ORDER BY purchase_cohort_month;


-- ============================================================
-- KPI 4 ── SIGNUP TO CATEGORY SELECTED
-- Definition : % of new signups who select an exam category
--              within N days of signing up. Tracked daily
--              for cohorts.
-- ============================================================

WITH

signups AS (
  SELECT
    user_id,
    date(date_add('minute', 330, CAST(signup_ts AS timestamp))) AS signup_date,
    date(date_add('minute', 330, CAST(category_selected_date AS timestamp))) AS cat_selected_date,
    string_examcategory_selected_525
  FROM dummy_db.users_base_table
  WHERE signup_date >= date('2025-01-01')
    AND signup_date <= date_add('day', -1, current_date)
),

funnel AS (
  SELECT
    signup_date,
    count(distinct user_id)                                                          AS total_signups,
    -- same day category selection
    count(distinct case when cat_selected_date = signup_date
                   then user_id end)                                                 AS cat_selected_d0,
    -- within 1 day
    count(distinct case when cat_selected_date <= date_add('day', 1, signup_date)
                   then user_id end)                                                 AS cat_selected_d1,
    -- within 3 days
    count(distinct case when cat_selected_date <= date_add('day', 3, signup_date)
                   then user_id end)                                                 AS cat_selected_d3,
    -- within 7 days
    count(distinct case when cat_selected_date <= date_add('day', 7, signup_date)
                   then user_id end)                                                 AS cat_selected_d7,
    -- category breakdown
    count(distinct case when string_examcategory_selected_525 = 'BANKING'   then user_id end) AS banking,
    count(distinct case when string_examcategory_selected_525 = 'SSC'       then user_id end) AS ssc,
    count(distinct case when string_examcategory_selected_525 = 'RAILWAYS'  then user_id end) AS railways,
    count(distinct case when string_examcategory_selected_525 = 'DEFENCE'   then user_id end) AS defence,
    count(distinct case when string_examcategory_selected_525 = 'UPSC'      then user_id end) AS upsc,
    count(distinct case when string_examcategory_selected_525 NOT IN (
        'BANKING','SSC','RAILWAYS','DEFENCE','UPSC')
        AND string_examcategory_selected_525 != '' then user_id end)                AS others
  FROM signups
  GROUP BY signup_date
)

SELECT
  'Signup to Category Selected' AS kpi,
  signup_date,
  total_signups,
  cat_selected_d0,
  round(100.0 * cat_selected_d0 / nullif(total_signups, 0), 2) AS pct_cat_selected_d0,
  cat_selected_d1,
  round(100.0 * cat_selected_d1 / nullif(total_signups, 0), 2) AS pct_cat_selected_d1,
  cat_selected_d3,
  round(100.0 * cat_selected_d3 / nullif(total_signups, 0), 2) AS pct_cat_selected_d3,
  cat_selected_d7,
  round(100.0 * cat_selected_d7 / nullif(total_signups, 0), 2) AS pct_cat_selected_d7,
  banking, ssc, railways, defence, upsc, others
FROM funnel
ORDER BY signup_date;


-- ============================================================
-- KPI 5 ── CONTENT CONSUMED PER DAY
-- Definition : Volume of content consumption events per day,
--              broken down by content type and exam category.
--              Includes DAU, avg items per user, and
--              total watch time.
-- ============================================================

WITH

daily_content AS (
  SELECT
    date(date_add('minute', 330, server_time))  AS activity_date,
    user_id,
    content_type,
    string_examcategory_selected_525,
    duration_seconds
  FROM dummy_db.content_consumed
  WHERE date(day)                                           >= date('2025-01-01')
    AND date(date_add('minute', 330, server_time))          >= date('2025-01-01')
    AND string_examcategory_selected_525 IN (
        'BANKING','SSC','RAILWAYS','DEFENCE','CTET','UPSC',
        'ENGINEERING','GATE','IIT JEE','NEET',
        'UTTAR PRADESH','BIHAR','RAJASTHAN','MAHARASHTRA',
        'MADHYA PRADESH','HARYANA','GUJARAT','PUNJAB_STATE_EXAMS',
        'TAMIL_NADU','KERALA','WEST_BENGAL'
    )
)

SELECT
  'Content Consumed Per Day' AS kpi,
  activity_date,

  -- Overall DAU and volume
  count(distinct user_id)                                    AS dau,
  count(*)                                                   AS total_consumption_events,
  round(count(*) * 1.0 / nullif(count(distinct user_id), 0), 2) AS avg_items_per_user,
  round(sum(duration_seconds) / 3600.0, 1)                  AS total_watch_hours,
  round(sum(duration_seconds) / nullif(count(distinct user_id), 0) / 60.0, 1) AS avg_watch_min_per_user,

  -- By content type
  count(distinct case when content_type = 'live_class'      then user_id end) AS live_class_users,
  count(case when content_type = 'live_class'               then 1 end)       AS live_class_events,
  count(distinct case when content_type = 'recorded_video'  then user_id end) AS video_users,
  count(case when content_type = 'recorded_video'           then 1 end)       AS video_events,
  count(distinct case when content_type = 'test'            then user_id end) AS test_users,
  count(case when content_type = 'test'                     then 1 end)       AS test_events,
  count(distinct case when content_type = 'notes'           then user_id end) AS notes_users,
  count(case when content_type = 'notes'                    then 1 end)       AS notes_events,
  count(distinct case when content_type = 'practice_quiz'   then user_id end) AS quiz_users,
  count(case when content_type = 'practice_quiz'            then 1 end)       AS quiz_events,

  -- Top categories by users
  count(distinct case when string_examcategory_selected_525 = 'BANKING'   then user_id end) AS banking_users,
  count(distinct case when string_examcategory_selected_525 = 'SSC'       then user_id end) AS ssc_users,
  count(distinct case when string_examcategory_selected_525 = 'RAILWAYS'  then user_id end) AS railways_users,
  count(distinct case when string_examcategory_selected_525 = 'DEFENCE'   then user_id end) AS defence_users,
  count(distinct case when string_examcategory_selected_525 = 'UPSC'      then user_id end) AS upsc_users

FROM daily_content
GROUP BY activity_date
ORDER BY activity_date;
