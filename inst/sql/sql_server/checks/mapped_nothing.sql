-- dummy to use for empty tables

SELECT * FROM (
                  select count(*)             AS ROW_NUM,
                         'NO MAPPED CONCEPTS' as "Concept Name",
                         0                    as "#Records",
                         0                    as "#Subjects"
                  from @cdmDatabaseSchema.cdm_source
              ) z
ORDER BY ROW_NUM
