-- mapped drugs

SELECT *
FROM (
	select ROW_NUMBER() OVER(ORDER BY count_big(drug_exposure_id) DESC) AS ROW_NUM,
       Cr.concept_name as "Concept Name",
       count_big(drug_exposure_id) as "#Records",
       count_big(distinct person_id) as "#Subjects"
       from @cdmDatabaseSchema.drug_exposure C
JOIN @vocabDatabaseSchema.CONCEPT CR
ON C.drug_concept_id = CR.CONCEPT_ID
group by CR.concept_name
) z
WHERE z.ROW_NUM <= 25
ORDER BY z.ROW_NUM
