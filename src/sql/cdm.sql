INSERT INTO SERGEI_BARANOVTUTBY.global_metrics
    (
        hk_global_metrics,
        date_update,
        currency_from,
        amount_total,
        cnt_transactions,
        avg_transactions_per_account,
        cnt_accounts_make_transactions,
        load_dt,
        load_src
    )
WITH cte_currencies_420 AS (
    -- на дату расчёта ближайшие в прошлое курсы к доллару
    -- {anchor_date_s} например 2022-10-05
    (
        SELECT DISTINCT
            (FIRST_VALUE(currency_code) OVER w)      as currency_code,
            (FIRST_VALUE(currency_code_with) OVER w) as currency_code_with,
            (FIRST_VALUE(currency_with_div) OVER w)  as currency_with_div,
            (FIRST_VALUE(date_update) OVER w)::date  as date_update
        FROM
            "SERGEI_BARANOVTUTBY__STAGING"."currencies"
        WHERE
            currency_code_with = 420 -- usa dollar
            AND date_update::date <= '{anchor_date_s}'::date
        WINDOW w AS (
            PARTITION BY currency_code
            ORDER BY date_update DESC
        )
    )
    UNION ALL
    (
        SELECT
            420 as currency_code,
            420 as currency_code_with,
            1.0 as currency_with_div,
            '{anchor_date_s}'::date as date_update
    )
)
SELECT
    hash(t.transaction_dt::date, t.currency_code)      as hk_global_metrics,
    t.transaction_dt::date                             as date_update,
    t.currency_code                                    as currency_from,
    SUM((t.amount * c.currency_with_div))              as amount_total,
    COUNT(*)                                           as cnt_transactions,
    (COUNT(*) / COUNT(DISTINCT t.account_number_from)) as avg_transactions_per_account,
    COUNT(DISTINCT t.account_number_from)              as cnt_accounts_make_transactions,
    now()                                              as load_dt,
    'project_final.py'                                 as load_src
FROM
    "SERGEI_BARANOVTUTBY__STAGING"."transactions" AS "t"
    INNER JOIN cte_currencies_420 AS "c" ON (
        "c"."currency_code" = "t"."currency_code"
        -- AND c.date_update::date = transaction_dt::date
    )
WHERE
    t.transaction_dt::date = '{anchor_date_s}'::date
    -- AND status = 'done'
    AND t.account_number_from > 0
    AND t.account_number_to > 0
    AND hash(t.transaction_dt::date, t.currency_code) NOT IN (
        SELECT hk_global_metrics FROM SERGEI_BARANOVTUTBY.global_metrics
    )
GROUP BY
    t.transaction_dt::date,
    t.currency_code
;