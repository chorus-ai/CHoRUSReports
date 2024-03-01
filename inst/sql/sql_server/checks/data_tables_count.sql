-- Clinical data table counts

select 'condition_era' as tablename, count_big(*) as count, count_big(distinct person_id) as "person count" from @cdmDatabaseSchema.condition_era
UNION
select 'condition_occurrence' as tablename, count_big(*) as count, count_big(distinct person_id) as "person count" from @cdmDatabaseSchema.condition_occurrence
UNION
select 'drug_exposure' as tablename, count_big(*) as count, count_big(distinct person_id) as "person count" from @cdmDatabaseSchema.drug_exposure
UNION
select 'death' as tablename, count_big(*) as count, count_big(distinct person_id) as "person count" from @cdmDatabaseSchema.death
UNION
select 'device_exposure' as tablename, count_big(*) as count, count_big(distinct person_id) as "person count" from @cdmDatabaseSchema.device_exposure
UNION
select 'dose_era' as tablename, count_big(*) as count, count_big(distinct person_id) as "person count" from @cdmDatabaseSchema.dose_era
UNION
select 'drug_era' as tablename, count_big(*) as count, count_big(distinct person_id) as "person count" from @cdmDatabaseSchema.drug_era
UNION
select 'measurement' as tablename, count_big(*) as count, count_big(distinct person_id) as "person count" from @cdmDatabaseSchema.measurement
UNION
select 'note' as tablename, count_big(*) as count, count_big(distinct person_id) as "person count" from @cdmDatabaseSchema.note
UNION
select 'observation' as tablename, count_big(*) as count, count_big(distinct person_id) as "person count" from @cdmDatabaseSchema.observation
UNION
select 'procedure_occurrence' as tablename, count_big(*) as count, count_big(distinct person_id) as "person count" from @cdmDatabaseSchema.procedure_occurrence
UNION
select 'specimen' as tablename, count_big(*) as count, count_big(distinct person_id) as "person count" from @cdmDatabaseSchema.specimen
UNION
select 'visit_detail' as tablename, count_big(*) as count, count_big(distinct person_id) as "person count" from @cdmDatabaseSchema.visit_detail
UNION
select 'visit_occurrence' as tablename, count_big(*) as count, count_big(distinct person_id) as "person count" from @cdmDatabaseSchema.visit_occurrence
