SELECT
    "TransactionID" AS transaction_id,
    "AccountID" AS account_id,
    "TransactionAmount" AS transaction_amount,
    "TransactionDate"::timestamp AS transaction_date,
    "TransactionType" AS transaction_type,
    "Location" AS location,
    "DeviceID" AS device_id,
    "Channel" AS channel,
    "CustomerAge" AS customer_age,
    "CustomerOccupation" AS customer_occupation,
    "TransactionDuration" AS transaction_duration,
    "LoginAttempts" AS login_attempts,
    "AccountBalance" AS account_balance,
    "PreviousTransactionDate"::timestamp AS previous_transaction_date,
    flag_excessive_logins,
    flag_large_amount_vs_balance,
    flag_rapid_large_transaction,
    flag_dormant_reactivation,
    fraud_score,
    is_flagged
FROM raw_transactions
WHERE "TransactionID" IS NOT NULL
