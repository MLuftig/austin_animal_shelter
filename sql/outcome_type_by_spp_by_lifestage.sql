/* 3 most common outcomes per spp per life stage*/
WITH data as (
   SELECT
	animal.cln_spp_outcome AS spp,
	animal.lifecycle_stage_outcome AS lifestage,
	outcome.outcome_type AS outcome,
	count(*) AS num 
   FROM
	animal
   JOIN
	outcome
   ON
	lower(animal.animal_id) = lower(outcome.animal_id)
   WHERE
	spp IN ('dog', 'cat') AND
   lifestage <> 'unknown'
   GROUP BY 
	cln_spp_outcome, lifecycle_stage_outcome, outcome_type
   ORDER BY
	1, 2, 3

	
),


ranked as(
   SELECT
	*,
	RANK() OVER (
		PARTITION BY spp, lifestage
		ORDER BY num DESC) as rnk
   FROM data
)

SELECT
   *
FROM
   ranked
WHERE
   rnk <= 3
ORDER BY
  spp,
  CASE lifestage
    WHEN 'juvenile' THEN 1
    WHEN 'young adult' THEN 2
    WHEN 'adult' THEN 3
    WHEN 'senior' THEN 4
    ELSE 5
  END,
  rnk,
  num DESC,
  outcome
;