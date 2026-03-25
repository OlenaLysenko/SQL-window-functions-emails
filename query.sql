SELECT
    DISTINCT sent_month,
    id_account,

    COUNT(id_message) OVER (
        PARTITION BY id_account, sent_month
    ) 
    / 
    COUNT(id_message) OVER (
        PARTITION BY sent_month
    ) * 100 AS sent_msg_percent_from_this_month,

    MIN(day_sent_date) OVER (
        PARTITION BY id_account, sent_month
    ) AS first_sent_date,

    MAX(day_sent_date) OVER (
        PARTITION BY id_account, sent_month
    ) AS last_sent_date

FROM (
    SELECT
        id_message,
        es.id_account AS id_account,

        DATE_ADD(s.date, INTERVAL es.sent_date DAY) AS day_sent_date,

        DATE_TRUNC(
            DATE_ADD(s.date, INTERVAL es.sent_date DAY),
            MONTH
        ) AS sent_month

    FROM data-analytics-mate.DA.email_sent es

    JOIN data-analytics-mate.DA.account_session acs
        ON es.id_account = acs.account_id

    JOIN data-analytics-mate.DA.session s
        ON acs.ga_session_id = s.ga_session_id
);

