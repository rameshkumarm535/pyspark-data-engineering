-- Create table
CREATE TABLE transactions (
    id INT,
    user_id INT,
    amount DOUBLE,
    trans_date DATE
);

-- Rolling 3 day window
WITH windowed AS (
    SELECT user_id,
           SUM(amount) OVER (
               PARTITION BY user_id
               ORDER BY trans_date
               RANGE BETWEEN INTERVAL 2 DAYS PRECEDING AND CURRENT ROW
           ) AS rolling_sum
    FROM transactions
)
SELECT DISTINCT user_id
FROM windowed
WHERE rolling_sum > 500;
