# @file ReportSections
#
# @author Tufts Medical Center
# @author Jared Houghtaling


#' Script to Create Sections of the Report
#'
#' @description
#' \code{ReportSections} defines logic for each of the proposed sections of the CHoRUS Report
#'
#' @details
#' \code{ReportSections} the proposed structure can be found here: https://github.com/chorus-ai/chorus-cloud-pm/issues/3#issuecomment-2069945869
#'
#' @param connectionDetails                An R object of type \code{connectionDetails} created using the function \code{createConnectionDetails} in the \code{DatabaseConnector} package.
#' @param connectionDetailsMerge           An R object of type \code{connectionDetails} created using the function \code{createConnectionDetails} in the \code{DatabaseConnector} package.
#' @param connectionDetailsOhdsi           An R object of type \code{connectionDetails} created using the function \code{createConnectionDetails} in the \code{DatabaseConnector} package.
#' @param cdm    	                         Abstracted cdm connection object from the CDMConnector package: https://github.com/darwin-eu/CDMConnector
#' @param cdmDatabaseSchema    	           Fully qualified name of database schema that contains OMOP CDM schema.
#'                                         On SQL Server, this should specifiy both the database and the schema, so for example, on SQL Server, 'cdm_instance.dbo'.
#' @param resultsDatabaseSchema		         Fully qualified name of database schema that we can write final results to. Default is cdmDatabaseSchema.
#'                                         On SQL Server, this should specifiy both the database and the schema, so for example, on SQL Server, 'cdm_results.dbo'.
#' @param vocabDatabaseSchema		           String name of database schema that contains OMOP Vocabulary. Default is cdmDatabaseSchema. On SQL Server, this should specifiy both the database and the schema, so for example 'results.dbo'.
#' @param oracleTempSchema                 For Oracle only: the name of the database schema where you want all temporary tables to be managed. Requires create/insert permissions to this database.
#' @param databaseId                       ID of your database, this will be used as subfolder for the results.
#' @param databaseName		                 String name of the database name. If blank, CDM_SOURCE table will be queried to try to obtain this.
#' @param databaseDescription              Provide a short description of the database.
#' @param outputFolder                     Path to store logs and SQL files
#' @param verboseMode                      Boolean to determine if the console will show all execution steps. Default = TRUE
#' @param accountUrl                       Web location of Azure storage account
#' @param accountKey                       SAS key for the Azure storage account
#' @param accountName                      Name of the blob container in question
#' @return                                 An object containing per-section tables and parameters for the given data source
#' @export

createReportSections <- function  (connectionDetails,
                                   connectionDetailsMerge,
                                   connectionDetailsOhdsi,
                                   cdm,
                                   cdmDatabaseSchema,
                                   databaseName = "",
                                   databaseId = "",
                                   outputFolder = "output",
                                   verboseMode = TRUE) {
  reportSections <- hash()
  TARGET_PAT_CNT <- 8000
  conn <- DatabaseConnector::connect(connectionDetails)
  connMerge <- DatabaseConnector::connect(connectionDetailsMerge)
  connOhdsi <- DatabaseConnector::connect(connectionDetailsOhdsi)
  
  
  if (databaseName == 'tufts') {
    containerName <- 'tuft'
  } else if (databaseName == 'pittsburgh') {
    containerName <- 'pitts'
  } else if (databaseName == 'florida') {
    containerName <- 'uflorida'
  } else if (databaseName == 'virginia') {
    containerName <- 'uva'
  } else {
    containerName <- databaseName
  }
  # SECTION 1 - Metadata & Overviews
  print("Generating: SECTION 1 - Metadata & Overviews")
  section1 <- hash()
  metadata <- hash()
  overview <- hash()
  patientCounts <- hash()
  differential <- hash()
  # allFiles <- list_blobs(accountUrl, accountKey, containerName)
  # allFiles <- allFiles %>% janitor::clean_names() # eliminate hash in names from Azure
  # dbWriteTable(conn, "public.allFiles", allFiles, overwrite = TRUE)
  if (databaseName != 'emory') {
    sqlManifest <- glue::glue("SELECT * FROM public.all_metadata_expanded WHERE container = '{containerName}'")
    fileManifest <- DatabaseConnector::querySql(
      connection = connOhdsi,
      sql = sqlManifest,
      snakeCaseToCamelCase = FALSE
    )
    dbWriteTable(conn, "public.allFiles", fileManifest, overwrite = TRUE)
  }
  sqlGrouped <- glue::glue("SELECT * FROM public.by_site_metadata WHERE container = '{containerName}'")
  fileGrouped <- DatabaseConnector::querySql(
    connection = connOhdsi,
    sql = sqlGrouped,
    snakeCaseToCamelCase = FALSE
  )
  dbWriteTable(conn, "public.groupedFiles", fileGrouped, overwrite = TRUE)
  sqlAllfiles <- glue::glue("
    ALTER TABLE public.allfiles ADD COLUMN person text;
    ALTER TABLE public.allfiles RENAME COLUMN mode TO modality;
    UPDATE public.allfiles
    SET person = CASE
           WHEN container = 'columbia' THEN split_part(name, '/', 2)
           WHEN container = 'duke' AND modality = 'WAVE' THEN split_part(name, '/', 1)
           WHEN container = 'duke' AND modality = 'IMAGE' THEN split_part(name, '/', 2)
           WHEN container = 'emory' THEN split_part(name, '/', 2)
           WHEN container = 'mayo' THEN split_part(split_part(name, '/', 2), '_', 1) -- No person in path
           WHEN container = 'mgh' THEN split_part(name, '/', 1) -- path leads with person for non-OMOP data
           WHEN container = 'mit' THEN split_part(name, '/', 2)
           WHEN container = 'nationwide' THEN split_part(name, '/', 1)
           WHEN container = 'pitts' THEN split_part(name, '/', 3)
           WHEN container = 'seattle' THEN split_part(name, '/', 1)
           WHEN container = 'tuft' THEN split_part(name, '/', 2)
           WHEN container = 'ucla' THEN replace(split_part(name, '/', 1), 'Person', '')
           WHEN container = 'uflorida' THEN split_part(name, '/', 1)
           WHEN container = 'uva' THEN split_part(name, '/', 2)
           END
  ")

  executeSql(conn, sqlAllfiles, progressBar = FALSE, reportOverallTime = FALSE)
  
  metadata[['deliveryTime']] <- fileGrouped$MOST_RECENT_UPLOAD[[1]] # get delivery time
  metadata[['packetSize']] <- querySql(conn,"select sum(size)/1000000000 FROM public.allfiles;")[[1]] # get delivery packet size in gb
  metadata[['numOfPriorDeliveries']] <- querySql(conn,"select count(*) FROM public.allreleases;")[[1]] # TODO get_prior_deliveries(databaseName)
  metadata[['recentFeedback']] <- c("NO FEEDBACK YET") # TODO get_latest_feedback(databaseName)
  section1[['Metadata']] <- metadata
  
  
  dqdOverview <- querySql(conn,"select  
                            round(sum(passed)/(sum(passed) + sum(failed))::numeric * 100, 2) as pcnt
                            FROM omopcdm.dqdashboard_results ;")[[1]]
  overview[['filesDelivered']] <- querySql(conn,"select count(*) FROM public.allfiles WHERE extension IS NOT NULL;")[[1]]
  overview[['qualityChecks']] <- dqdOverview
  overview[['dqdFailures']]   <- querySql(conn, "select count(*) FROM omopcdm.dqdashboard_results WHERE failed = '1';")[[1]]
  #overview[['phiIssuesOMOP']] <- querySql(conn,"select count(*) from omopcdm.phi_output WHERE pred_result = '1';")[[1]]
  #overview[['phiIssuesWAVE']] <- "TBD"
  #overview[['phiIssuesIMAG']] <- "TBD"
  #overview[['phiIssuesNOTE']] <- "TBD"
  overview[['chorusQC']] <- 'TBD' # TODO get_chorus_qc(databaseName)
  overview[['chorusChars']] <- 'TBD'
  section1[['Overview']] <- overview
  
  
  
  waveCounts <- querySql(conn,glue::glue("select count(DISTINCT person) from public.allfiles WHERE lower(modality) LIKE 'wave%' AND extension IS NOT NULL;"))[[1]]
  waveFileCounts <- querySql(conn,glue::glue("select count(*) from public.allfiles WHERE lower(modality) LIKE 'wave%' AND extension IS NOT NULL;"))[[1]]
  imgCounts <- querySql(conn,"select count(DISTINCT person) from public.allfiles WHERE lower(modality) LIKE 'imag%' AND extension IS NOT NULL;")[[1]]
  imgFileCounts <- querySql(conn,"select count(*) from public.allfiles WHERE lower(modality) LIKE 'imag%' AND extension IS NOT NULL;")[[1]]
  omopCounts <- querySql(conn,"SELECT COUNT(*) FROM omopcdm.person")[[1]]
  omopFileCounts <- querySql(conn,"select count(*) from public.allfiles WHERE (lower(modality) LIKE 'omop%' OR lower(person) = 'omop') AND extension IS NOT NULL;")[[1]]
  noteCounts <- querySql(conn,"SELECT COUNT(DISTINCT(person_id)) FROM omopcdm.note")[[1]]
  personsAnyModes <- max(waveCounts, imgCounts, omopCounts) #TODO Add noteCounts eventually
  personsAllModes <- min(waveCounts, imgCounts, omopCounts) #TODO Add noteCounts eventually
  
  
  patientCounts[['anyDataPersons']] <- personsAnyModes
  patientCounts[['anyDataFiles']] <- omopFileCounts + imgFileCounts + waveFileCounts
  patientCounts[['anyDataPersonsPct']] <- (personsAnyModes / TARGET_PAT_CNT)*100
  patientCounts[['allDataPersons']] <- personsAllModes
  patientCounts[['allDataFiles']] <- omopFileCounts + imgFileCounts + waveFileCounts
  patientCounts[['allDataPersonsPct']] <- (personsAllModes / TARGET_PAT_CNT)*100
  patientCounts[['omopDataFiles']] <- omopFileCounts
  patientCounts[['omopDataPersons']] <- omopCounts
  patientCounts[['omopDataPersonsPct']] <- (omopCounts / TARGET_PAT_CNT)*100
  patientCounts[['imageDataFiles']] <- imgFileCounts
  patientCounts[['imageDataPersons']] <- imgCounts
  patientCounts[['imageDataPersonsPct']] <- (imgCounts / TARGET_PAT_CNT)*100
  patientCounts[['waveDataFiles']] <- waveFileCounts
  patientCounts[['waveDataPersons']] <- waveCounts
  patientCounts[['waveDataPersonsPct']] <- (waveCounts / TARGET_PAT_CNT)*100
  patientCounts[['noteDataPersons']] <- noteCounts
  patientCounts[['noteDataFiles']] <- querySql(conn,"SELECT COUNT(*) FROM omopcdm.note")[[1]]
  patientCounts[['noteDataPersonsPct']] <- (noteCounts / TARGET_PAT_CNT)*100
  #approvedPersons <- 0 # TODO get_approved_persons()
  #patientCounts[['allDataApprovedPersons']] <- approvedPersons
  #patientCounts[['allDataApprovedFiles']] <- 0 # get_approved_files
  #patientCounts[['allDataApprovedPersonsPct']] <- (approvedPersons / TARGET_PAT_CNT)*100
  #patientCounts[['allDataApprovedPersonsDelta']] <- 0 # get_patient_counts_delta('approved')
  #deidPersons <- 0 # TODO get_deid_persons()
  #patientCounts[['allDataApprovedDeidPersons']] <- deidPersons
  #patientCounts[['allDataApprovedDeidFiles']] <- 0 # get_approved_deid_files
  #patientCounts[['allDataApprovedDeidPersonsPct']] <- (deidPersons / TARGET_PAT_CNT)*100
  #patientCounts[['allDataApprovedDeidPersonsDelta']] <- 0 # get_patient_counts_delta('deid')
  
  if (metadata$numOfPriorDeliveries >= 1) {
    prior_oid <- querySql(conn,"select result_oid FROM public.allreleases ORDER BY num_prior_delivs DESC LIMIT 1;")[[1]]
    prior_results <- getPriorReleaseRds(connectionDetails, prior_oid, databaseName)
    patientCounts[['allDataPersonsDelta']] <- personsAllModes - prior_results$section1$PatientCounts$allDataPersons
    patientCounts[['anyDataPersonsDelta']] <- personsAnyModes - prior_results$section1$PatientCounts$anyDataPersons
    patientCounts[['omopDataPersonsDelta']] <- omopCounts - prior_results$section1$PatientCounts$omopDataPersons
    patientCounts[['imageDataPersonsDelta']] <- imgCounts - prior_results$section1$PatientCounts$imageDataPersons
    patientCounts[['waveDataPersonsDelta']] <- waveCounts - prior_results$section1$PatientCounts$waveDataPersons
    patientCounts[['noteDataPersonsDelta']] <- noteCounts - prior_results$section1$PatientCounts$noteDataPersons
  } else {
    patientCounts[['allDataPersonsDelta']] <- personsAllModes
    patientCounts[['anyDataPersonsDelta']] <- personsAnyModes
    patientCounts[['omopDataPersonsDelta']] <- omopCounts
    patientCounts[['imageDataPersonsDelta']] <- imgCounts
    patientCounts[['waveDataPersonsDelta']] <- waveCounts
    patientCounts[['noteDataPersonsDelta']] <- noteCounts
  }
  
  section1[['PatientCounts']] <- patientCounts
  
  reportSections[['section1']] <- section1
  print("SECTION 1 generated!")
  
#   # SECTION 2 - OMOP Characterization
#   print("Generating: SECTION 2 - OMOP Characterization")
#
#   section2 <- hash()
#   omopOverview <- hash()
#   omopPlots <- hash()
#
#   # Create OMOP Characterization report using OmopSketch -> https://ohdsi.github.io/OmopSketch/index.html
#
#   omopOverview$condition_occurrence <- OmopSketch::summariseClinicalRecords(cdm, c("condition_occurrence"))
#   omopOverview$device_exposure <- OmopSketch::summariseClinicalRecords(cdm, c("device_exposure"))
#   omopOverview$drug_exposure <- OmopSketch::summariseClinicalRecords(cdm, c("drug_exposure"))
#   omopOverview$observation <- OmopSketch::summariseClinicalRecords(cdm, c("observation"))
#   omopOverview$measurement <- OmopSketch::summariseClinicalRecords(cdm, c("measurement"))
#   omopOverview$procedure_occurrence <- OmopSketch::summariseClinicalRecords(cdm, c("procedure_occurrence"))
#   omopOverview$visit_occurrence <- OmopSketch::summariseClinicalRecords(cdm, c("visit_occurrence"))
#   omopOverview$visit_detail <- OmopSketch::summariseClinicalRecords(cdm, c("visit_detail"))
#
  section2[['omopOverview']] <- NULL
  
  sqlDataDensity <- glue::glue("
  select t1.table_name as SERIES_NAME,
	t1.stratum_1 as X_CALENDAR_MONTH,
	FLOOR(t1.stratum_1/100) AS X_CALENDAR_YEAR,
	NULLIF(round(1.0*t1.count_value/denom.count_value,2), 0) as Y_RECORD_COUNT
  from
  (
  	select 'Visit occurrence' as table_name, CAST(stratum_1 as bigint) stratum_1, count_value from omopcdm.achilles_results where analysis_id = 220 GROUP BY analysis_id, stratum_1, count_value
  	union all
  	select 'Condition occurrence' as table_name, CAST(stratum_1 as bigint) stratum_1, count_value from omopcdm.achilles_results where analysis_id = 420 GROUP BY analysis_id, stratum_1, count_value
  	union all
  	select 'Death' as table_name, CAST(stratum_1 as bigint) stratum_1, count_value from omopcdm.achilles_results where analysis_id = 502 GROUP BY analysis_id, stratum_1, count_value
  	union all
  	select 'Procedure occurrence' as table_name, CAST(stratum_1 as bigint) stratum_1, count_value from omopcdm.achilles_results where analysis_id = 620 GROUP BY analysis_id, stratum_1, count_value
  	union all
  	select 'Drug exposure' as table_name, CAST(stratum_1 as bigint) stratum_1, count_value from omopcdm.achilles_results where analysis_id = 720 GROUP BY analysis_id, stratum_1, count_value
  	union all
  	select 'Observation' as table_name, CAST(stratum_1 as bigint) stratum_1, count_value from omopcdm.achilles_results where analysis_id = 820 GROUP BY analysis_id, stratum_1, count_value
  	union all
  	select 'Drug era' as table_name, CAST(stratum_1 as bigint) stratum_1, count_value from omopcdm.achilles_results where analysis_id = 920 GROUP BY analysis_id, stratum_1, count_value
  	union all
  	select 'Condition era' as table_name, CAST(stratum_1 as bigint) stratum_1, count_value from omopcdm.achilles_results where analysis_id = 1020 GROUP BY analysis_id, stratum_1, count_value
  	union all
  	select 'Observation period' as table_name, CAST(stratum_1 as bigint) stratum_1, count_value from omopcdm.achilles_results where analysis_id = 111 GROUP BY analysis_id, stratum_1, count_value
  	union all
  	select 'Measurement' as table_name, CAST(stratum_1 as bigint) stratum_1, count_value from omopcdm.achilles_results where analysis_id = 1820 GROUP BY analysis_id, stratum_1, count_value
  ) t1
  inner join
  (select CAST(stratum_1 as bigint) stratum_1, count_value from omopcdm.achilles_results where analysis_id = 117 GROUP BY analysis_id, stratum_1, count_value) denom
  on t1.stratum_1 = denom.stratum_1
  WHERE t1.stratum_1 > 202000
  AND t1.stratum_1 < 202500
  ORDER BY SERIES_NAME, t1.stratum_1
  ")
  # <- DatabaseConnector::querySql(conn, sqlDataDensity) 
  densityDataSite <- DatabaseConnector::querySql(conn, sqlDataDensity) 
  densityDataMerge <- DatabaseConnector::querySql(connMerge, sqlDataDensity) 
  densityPlotSite <- ggplot(densityDataSite, aes(x=X_CALENDAR_MONTH, y=Y_RECORD_COUNT, colour = SERIES_NAME), axis.labels = "all_y") + 
    geom_point() +
    facet_grid(cols = vars(X_CALENDAR_YEAR), scales = "free",axis.labels = "all_y") +
    theme(axis.text.x=element_blank(),axis.ticks.x=element_blank()) + scale_y_continuous(name="Record Counts Per Person", trans="log10") +
    scale_x_continuous(name="Month of Observation") +
    ggtitle(label=paste(str_to_upper(databaseName),"records per person")) +
    labs(colour = "OMOP Table") 
  densityPlotMerge <- ggplot(densityDataMerge, aes(x=X_CALENDAR_MONTH, y=Y_RECORD_COUNT, colour = SERIES_NAME), axis.labels = "all_y") + 
    geom_point() +
    facet_grid(cols = vars(X_CALENDAR_YEAR), scales = "free",axis.labels = "all_y") +
    theme(axis.text.x=element_blank(),axis.ticks.x=element_blank()) + scale_y_continuous(name="Record Counts Per Person", trans="log10") +
    scale_x_continuous(name="Month of Observation") +
    ggtitle(label="MERGE records per person") +
              labs(colour = "OMOP Table") 
  
  if (nrow(densityDataSite) == 0) {
    densityDummy <- data.frame(SERIES_NAME=c("Death"),X_CALENDAR_MONTH=c(202001), X_CALENDAR_YEAR=c(2020), Y_RECORD_COUNT=c(1))
    densityPlotDummy <- ggplot(densityDummy, aes(x=X_CALENDAR_MONTH, y=Y_RECORD_COUNT, colour = SERIES_NAME), axis.labels = "all_y") + 
      geom_point() +
      facet_grid(cols = vars(X_CALENDAR_YEAR), scales = "free",axis.labels = "all_y") +
      theme(axis.text.x=element_blank(),axis.ticks.x=element_blank()) + scale_y_continuous(name="Record Counts Per Person", trans="log10") +
      scale_x_continuous(name="Month of Observation") +
      ggtitle(label="No Records in Expected Date Range") +
      labs(colour = "OMOP Table") 
    densityPlot <- densityPlotDummy + densityPlotMerge
  } else {
    densityPlot <- densityPlotSite + densityPlotMerge
  }
  omopPlots[['densityPlot']] <- densityPlot
  
 sqlPie <- "
   SELECT 'condition_occurrence' AS omop_table,
         sum(count_value) as cnt
   FROM omopcdm.achilles_results WHERE analysis_id = 401
   UNION
   SELECT 'device_exposure' AS omop_table,
         sum(count_value) as cnt
   FROM omopcdm.achilles_results WHERE analysis_id = 2101
   UNION
   SELECT 'drug_exposure' AS omop_table, 
         sum(count_value) as cnt
   FROM omopcdm.achilles_results WHERE analysis_id = 701
   UNION
   SELECT 'observation' AS omop_table,
         sum(count_value) as cnt
   FROM omopcdm.achilles_results WHERE analysis_id = 801
   UNION
   SELECT 'measurement' AS omop_table,
         sum(count_value) as cnt
   FROM omopcdm.achilles_results WHERE analysis_id = 1801
   UNION
   SELECT 'procedure_occurrence' AS omop_table,
         sum(count_value) as cnt
   FROM omopcdm.achilles_results WHERE analysis_id =601
   "
  piePlot <- DatabaseConnector::querySql(conn, sqlPie)
  piePlotMerge <- DatabaseConnector::querySql(connMerge, sqlPie)
  dataPieSite <- piePlot %>% 
   arrange(desc(CNT)) %>%
   mutate(prop = CNT / sum(piePlot$CNT) * 100) %>%
   mutate(posy = cumsum(prop) - prop/2)
  
  dataPieMerge <- piePlotMerge %>% 
    arrange(desc(CNT)) %>%
    mutate(prop = CNT / sum(piePlotMerge$CNT) * 100) %>%
    mutate(posy = cumsum(prop) - prop/2)
  
  sitePiePlot <- ggplot(dataPieSite, aes(x = "", y = CNT, fill = OMOP_TABLE)) +
    geom_bar(stat = "identity") +
    coord_polar("y")+ 
    geom_label_repel(aes(label = paste(round(CNT/1000000, digits=3), 'M')), position = position_stack(vjust = 0.5), show.legend = FALSE) +
    ggtitle(label=paste(str_to_upper(databaseName), "rows per domain")) +
    theme_void()
  
  mergePiePlot <- ggplot(dataPieMerge, aes(x = "", y = CNT, fill = OMOP_TABLE)) +
    geom_bar(stat = "identity") +
    coord_polar("y")+ 
    geom_label_repel(aes(label = paste(round(CNT/1000000, digits=3), 'M')), position = position_stack(vjust = 0.5), show.legend = FALSE) +
    ggtitle(label="MERGE rows per domain") +
    theme_void()
  
  piePlot <- sitePiePlot + mergePiePlot
  omopPlots[['piePlot']] <- piePlot
  
    
  section2[['omopPlots']] <- omopPlots
  
  reportSections[['section2']] <- section2
  
  print("SECTION 2 generated!")
  # SECTION 3 - Data Quality
  print("Generating: SECTION 3 - Data Quality")
  
  section3 <- hash()
  dqdResults <- hash()
  
  dqdResults[['qualityPerCheck']] <- querySql(conn, "select cdm_table_name, category, count(*) AS count_failed FROM omopcdm.dqdashboard_results WHERE failed = '1' GROUP BY cdm_table_name, category ORDER BY 3 DESC;")
  dqdResults[['allResults']] <- querySql(conn, "select * FROM omopcdm.dqdashboard_results;")
  
  section3[['DQDResults']] <- dqdResults
  
  reportSections[['section3']] <- section3
  print("SECTION 3 generated!")
  # SECTION 4 - Cohorts and Characterization
  print("Generating: SECTION 4 - Cohorts and Characterization")
  section4 <- hash()
  cohortsCounts <- hash()
  delphiCounts <- hash()
  
  sqlDelphi <- glue::glue("
  DROP TABLE IF EXISTS public.delphi_capture;
  CREATE TABLE public.delphi_capture AS (
  WITH event_concepts AS (
  SELECT condition_concept_id AS event_concept_id, count(*) AS cnt FROM {cdmDatabaseSchema}.condition_occurrence t
  INNER JOIN public.delphi_concepts_unique d
  ON d.del_concept_id = t.condition_concept_id 
  GROUP BY condition_concept_id
  UNION
  SELECT device_concept_id AS event_concept_id, count(*) AS cnt FROM {cdmDatabaseSchema}.device_exposure t
  INNER JOIN public.delphi_concepts_unique d
  ON d.del_concept_id = t.device_concept_id 
  GROUP BY device_concept_id
  UNION
  SELECT drug_concept_id AS event_concept_id, count(*) AS cnt FROM {cdmDatabaseSchema}.drug_exposure t
  INNER JOIN public.delphi_concepts_unique d
  ON d.del_concept_id = t.drug_concept_id 
  GROUP BY drug_concept_id
  UNION
  SELECT measurement_concept_id AS event_concept_id, count(*) AS cnt FROM {cdmDatabaseSchema}.measurement t
  INNER JOIN public.delphi_concepts_unique d
  ON d.del_concept_id = t.measurement_concept_id 
  GROUP BY measurement_concept_id
  UNION
  SELECT observation_concept_id AS event_concept_id, count(*) AS cnt FROM {cdmDatabaseSchema}.observation t
  INNER JOIN public.delphi_concepts_unique d
  ON d.del_concept_id = t.observation_concept_id 
  GROUP BY observation_concept_id
  UNION
  SELECT procedure_concept_id AS event_concept_id, count(*) AS cnt FROM {cdmDatabaseSchema}.procedure_occurrence t
  INNER JOIN public.delphi_concepts_unique d
  ON d.del_concept_id = t.procedure_concept_id
  GROUP BY procedure_concept_id
  )
  SELECT c.concept_id AS delphi_concept_id, 
         c.concept_name, 
         c.domain_id, 
         COALESCE(e.cnt,0) AS cnt
  FROM public.delphi_concepts_unique d 
  LEFT JOIN event_concepts e
  ON e.event_concept_id = d.del_concept_id
  INNER JOIN {cdmDatabaseSchema}.concept c
  ON d.del_concept_id = c.concept_id
  ORDER BY 4 DESC
  );
  ")
  
  sqlDelphiGrouped <- "
  WITH groupDelphi AS (
    select domain_id, 
           COUNT(*) AS distinct_concepts_total, 
           COUNT(DISTINCT CASE WHEN cnt > 0 THEN delphi_concept_id ELSE NULL END) AS captured_concepts_total
    FROM public.delphi_capture
    GROUP BY domain_id)
    SELECT domain_id,
           distinct_concepts_total,
           captured_concepts_total,
           ((captured_concepts_total::float/distinct_concepts_total::float) * 100)::Decimal(3,1) AS captured_percent
    FROM groupDelphi
    ORDER BY 4 DESC;
  "
  
  executeSql(conn, sqlDelphi, progressBar = FALSE, reportOverallTime = FALSE)
  
  delphiCounts[['delphiCountsAll']] <- querySql(conn, "select * FROM public.delphi_capture;")
  delphiCounts[['delphiGrouped']] <- querySql(conn, sqlDelphiGrouped)
  delphiCounts[['delphiPercentCapture']] <- querySql(conn, "select ((COUNT(DISTINCT CASE WHEN cnt > 0 THEN delphi_concept_id ELSE NULL END)::float/count(*)::float) * 100)::decimal(3,1) FROM public.delphi_capture;")[[1]]
  cohortsCounts[['topInNetwork']] <- querySql(connMerge,glue::glue("select description, cnt_merge, percent_merge, cnt_{databaseName}, ROUND((cnt_{databaseName} / (SELECT count(*) FROM omopcdm.person WHERE src_name = '{databaseName}'))*100, 1) AS percent_{databaseName} from omopcdm.cohort_reports ORDER BY cnt_merge DESC limit 20;"))
  cohortsCounts[['topInSource']] <- querySql(connMerge,glue::glue("select description, cnt_merge, percent_merge, cnt_{databaseName}, ROUND((cnt_{databaseName} / (SELECT count(*) FROM omopcdm.person WHERE src_name = '{databaseName}'))*100, 1) AS percent_{databaseName} from omopcdm.cohort_reports ORDER BY cnt_{databaseName} DESC limit 20;"))
  
  section4[['CohortCounts']] <- cohortsCounts
  section4[['DelphiCounts']] <- delphiCounts
  reportSections[['section4']] <- section4
  print("SECTION 4 generated!")
  
  disconnect(conn)
  disconnect(connMerge)
  return(reportSections)
}
    