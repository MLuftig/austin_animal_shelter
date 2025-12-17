/*AVG LOS per spp*/
SELECT	
	animal.cln_spp_outcome,
	AVG(los.length_of_stay_days) avg_stay_days
FROM	
	animal
JOIN los
ON animal.animal_id = los.animal_id
GROUP BY 1
ORDER BY 2 DESC;
	