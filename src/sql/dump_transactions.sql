-- dumps day trsnsactions
WITH "cte_json" AS (
    SELECT
        "object_id",
        "sent_dttm",
        cast("payload" AS json) AS "payload_json"
    FROM
        "stg"."transactions"
)
SELECT DISTINCT ON ("object_id")
    "object_id"                            AS "object_id",
    "sent_dttm"                            AS "sent_dttm",
    "payload_json"->>'operation_id'        AS "operation_id",
    "payload_json"->>'account_number_from' AS "account_number_from",
    "payload_json"->>'account_number_to'   AS "account_number_to",
    "payload_json"->>'currency_code'       AS "currency_code",
    "payload_json"->>'country'             AS "country",
    "payload_json"->>'status'              AS "status",
    "payload_json"->>'transaction_type'    AS "transaction_type",
    "payload_json"->>'amount'              AS "amount",
    "payload_json"->>'transaction_dt'      AS "transaction_dt"
FROM
    "cte_json"
WHERE
    ("payload_json"->>'transaction_dt')::date = '{0}'::date
-- without ';': it is for "COPY (...) TO STDOUT WITH CSV DELIMITER ','"
