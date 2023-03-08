-- DROP TABLE IF EXISTS "SERGEI_BARANOVTUTBY__STAGING"."currencies";
CREATE TABLE IF NOT EXISTS "SERGEI_BARANOVTUTBY__STAGING"."currencies" (
    object_id UUID,
    sent_dttm TIMESTAMP(3),
    date_update TIMESTAMP(0),
    currency_code INTEGER,
    currency_code_with INTEGER,
    currency_with_div NUMERIC(8,2),
    CONSTRAINT currencies_pkey PRIMARY KEY (object_id)
)
ORDER BY date_update, object_id
SEGMENTED BY HASH(date_update, object_id) ALL NODES
PARTITION BY "date_update"::date
GROUP BY calendar_hierarchy_day("date_update"::date, 3, 2)
;

-- DROP TABLE IF EXISTS "SERGEI_BARANOVTUTBY__STAGING"."transactions";
CREATE TABLE IF NOT EXISTS "SERGEI_BARANOVTUTBY__STAGING"."transactions" (
    object_id UUID,
    sent_dttm TIMESTAMP(3),
    operation_id UUID,
    account_number_from INTEGER,
    account_number_to INTEGER,
    currency_code INTEGER,
    country VARCHAR(64),
    status VARCHAR(32),
    transaction_type VARCHAR(32),
    amount INTEGER,
    transaction_dt TIMESTAMP(0),
    CONSTRAINT transactions_pkey PRIMARY KEY (object_id)
)
ORDER BY transaction_dt, object_id
SEGMENTED BY HASH(transaction_dt, object_id) ALL NODES
PARTITION BY "transaction_dt"::date
GROUP BY calendar_hierarchy_day("transaction_dt"::date, 3, 2)
;

-- DROP TABLE IF EXISTS "SERGEI_BARANOVTUTBY"."global_metrics";
CREATE TABLE "SERGEI_BARANOVTUTBY"."global_metrics" (
    hk_global_metrics INTEGER PRIMARY KEY, -- hash(date_update, currency_from)
    date_update DATE NOT NULL,
    currency_from INTEGER NOT NULL,
    amount_total NUMERIC(18,2) NOT NULL,
    cnt_transactions INTEGER NOT NULL,
    avg_transactions_per_account NUMERIC(12,4) NOT NULL,
    cnt_accounts_make_transactions INTEGER NOT NULL,
    load_dt DATETIME NOT NULL,
    load_src VARCHAR(32) NOT NULL
)
ORDER BY date_update, currency_from
SEGMENTED BY hk_global_metrics ALL NODES
PARTITION BY "date_update"::date
GROUP BY calendar_hierarchy_day("date_update"::date, 3, 2)
;