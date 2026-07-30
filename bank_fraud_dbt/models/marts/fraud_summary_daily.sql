SELECT
    DATE(transaction_date) AS transaction_day,
    COUNT(*) AS total_transactions,
    SUM(CASE WHEN is_flagged THEN 1 ELSE 0 END) AS flagged_transactions,
    ROUND(
        100.0 * SUM(CASE WHEN is_flagged THEN 1 ELSE 0 END) / COUNT(*), 2
    ) AS flagged_percentage,
    SUM(transaction_amount) AS total_amount,
    SUM(CASE WHEN is_flagged THEN transaction_amount ELSE 0 END) AS flagged_amount,
    ROUND(AVG(fraud_score), 2) AS avg_fraud_score
FROM {{ ref('stg_transactions') }}
GROUP BY DATE(transaction_date)
ORDER BY transaction_day
