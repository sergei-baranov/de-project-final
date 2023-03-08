-- dumps day currencies updates
WITH "cte_json" AS (
    SELECT
        "object_id",
        "sent_dttm",
        cast("payload" AS json) AS "payload_json"
    FROM
        "stg"."currencies"
)
SELECT DISTINCT ON ("object_id")
    "object_id"                           AS "object_id",
    "sent_dttm"                           AS "sent_dttm",
    "payload_json"->>'date_update'        AS "date_update",
    "payload_json"->>'currency_code'      AS "currency_code",
    "payload_json"->>'currency_code_with' AS "currency_code_with",
    "payload_json"->>'currency_with_div'  AS "currency_with_div"
FROM
    "cte_json"
WHERE
    ("payload_json"->>'date_update')::date = '{0}'::date
-- without ';': it is for "COPY (...) TO STDOUT WITH CSV DELIMITER ','"
