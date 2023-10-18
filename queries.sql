-- 1
SELECT animal_type, COUNT(DISTINCT animal_id) AS num_animals
FROM dim_animal
GROUP BY animal_type;



-- 2
WITH AnimalOutcomeCounts AS (
    SELECT animal_id, COUNT(DISTINCT outcome_id) AS num_outcomes
    FROM fact_animal
    GROUP BY animal_id
)
SELECT COUNT(*) AS num_animals
FROM AnimalOutcomeCounts
WHERE num_outcomes > 1;


-- 3
SELECT month, COUNT(*) AS num_outcomes
FROM dim_date
GROUP BY month
ORDER BY num_outcomes DESC
LIMIT 5;


--4 a
SELECT age_group, COUNT(*) AS num_cats
FROM (
    SELECT a.animal_id,
        CASE
            WHEN date_part('year', current_date) - date_part('year', a.date_of_birth) < 1 THEN 'Kitten'
            WHEN date_part('year', current_date) - date_part('year', a.date_of_birth) > 10 THEN 'Senior'
            ELSE 'Adult'
        END AS age_group
    FROM dim_animal a
    JOIN fact_animal fa ON a.animal_id = fa.animal_id
    JOIN dim_outcome o ON fa.outcome_id = o.outcome_id
    WHERE a.animal_type = 'Cat' AND o.outcome_type = 'Adopted'
) AS adopted_cats
GROUP BY age_group;



-- 4 b
WITH CumulativeOutcomes AS (
    SELECT d.date, COUNT(*) AS cumulative_outcomes
    FROM dim_date d
    JOIN fact_animal fa ON d.date_id = fa.date_id
    GROUP BY d.date
)
SELECT date, cumulative_outcomes
FROM CumulativeOutcomes
ORDER BY date;


-- 5
WITH CumulativeOutcomes AS (
    SELECT d.date, COUNT(*) AS cumulative_outcomes
    FROM dim_date d
    JOIN fact_animal fa ON d.date_id = fa.date_id
    GROUP BY d.date
)
SELECT date, cumulative_outcomes
FROM CumulativeOutcomes
ORDER BY date;

