SELECT us_state, tupleElement(winner, 1) AS cat_id,
       tupleElement(winner, 2) AS max_amount
FROM
(
    SELECT us_state, argMaxMerge(winner) AS winner
    FROM hw2.state_max GROUP BY us_state
)
ORDER BY us_state
FORMAT CSVWithNames;
