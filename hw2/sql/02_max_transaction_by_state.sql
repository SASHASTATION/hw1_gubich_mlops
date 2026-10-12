-- Category of the largest individual transaction, not the largest category sum.
-- Equal amounts: lexicographically greatest cat_id, then transaction_id.
SELECT us_state, tupleElement(winner, 1) AS cat_id,
       tupleElement(winner, 2) AS max_amount
FROM
(
    SELECT us_state,
           argMax(tuple(cat_id, amount), tuple(amount, cat_id, transaction_id)) AS winner
    FROM hw2.transactions GROUP BY us_state
)
ORDER BY us_state
FORMAT CSVWithNames;
