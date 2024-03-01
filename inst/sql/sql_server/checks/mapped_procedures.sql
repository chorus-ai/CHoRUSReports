-- mapped procedures

SELECT *
FROM (
	select ROW_NUMBER() OVER(ORDER BY count_big(procedure_occurrence_id) DESC) AS ROW_NUM,
       Cr.concept_name as "Concept Name",
       count_big(procedure_occurrence_id) as "#Records",
       count_big(distinct person_id) as "#Subjects"
       from @cdmDatabaseSchema.procedure_occurrence C
JOIN @vocabDatabaseSchema.CONCEPT CR
ON C.procedure_concept_id = CR.CONCEPT_ID
group by CR.concept_name
) z
WHERE z.ROW_NUM <= 25
ORDER BY z.ROW_NUM
