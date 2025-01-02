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
#' @param depth                            Depth of the directory structure in the Azure container (should be 3 per convention, but can vary in practice)
#' @return                                 An object containing per-section tables and parameters for the given data source
#' @export

createReportSections <- function  (connectionDetails,
                                   connectionDetailsMerge,
                                   connectionDetailsOhdsi,
                                   cdmDatabaseSchema,
                                   databaseName = "",
                                   databaseId = "",
                                   outputFolder = "output",
                                   verboseMode = TRUE,
                                   depth = 3) {
  reportSections <- hash()
  TARGET_PAT_CNT <- 8000
  conn <- connect(connectionDetails)
  connMerge <- connect(connectionDetailsMerge)
  connOhdsi <- connect(connectionDetailsOhdsi)
  
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
  
  section1 <- hash()
  metadata <- hash()
  overview <- hash()
  patientCounts <- hash()
  differential <- hash()
  # allFiles <- list_blobs(accountUrl, accountKey, containerName)
  # allFiles <- allFiles %>% janitor::clean_names() # eliminate hash in names from Azure
  # dbWriteTable(conn, "public.allFiles", allFiles, overwrite = TRUE)
  sqlManifest <- glue::glue("SELECT * FROM public.all_metadata_expanded WHERE container = '{containerName}'") 
  fileManifest <- DatabaseConnector::querySql(
    connection = connOhdsi,
    sql = sqlManifest,
    snakeCaseToCamelCase = FALSE
  )
  dbWriteTable(conn, "public.allFiles", fileManifest, overwrite = TRUE)
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
    SET person = NULLIF(split_part(name, '/', {depth}-2), '')
    WHERE name IS NOT NULL;
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
  noteCounts <- querySql(conn,"SELECT COUNT(*) FROM omopcdm.note")[[1]]
  personsAnyModes <- max(waveCounts, imgCounts, omopCounts) #TODO Add noteCounts eventually
  personsAllModes <- min(waveCounts, imgCounts, omopCounts) #TODO Add noteCounts eventually
  
  
  patientCounts[['anyDataPersons']] <- personsAnyModes
  patientCounts[['anyDataFiles']] <- omopFileCounts + imgFileCounts + waveFileCounts
  patientCounts[['anyDataPersonsPct']] <- (personsAnyModes / TARGET_PAT_CNT)*100
  patientCounts[['anyDataPersonsDelta']] <- 0 # get_patient_counts_delta('any')
  patientCounts[['allDataPersons']] <- personsAllModes
  patientCounts[['allDataFiles']] <- omopFileCounts + imgFileCounts + waveFileCounts
  patientCounts[['allDataPersonsPct']] <- (personsAllModes / TARGET_PAT_CNT)*100
  patientCounts[['allDataPersonsDelta']] <- 0 # get_patient_counts_delta('all')
  patientCounts[['omopDataFiles']] <- omopFileCounts
  patientCounts[['omopDataPersons']] <- omopCounts
  patientCounts[['omopDataPersonsPct']] <- (omopCounts / TARGET_PAT_CNT)*100
  patientCounts[['omopDataPersonsDelta']] <- 0 # get_patient_counts_delta('omop')
  patientCounts[['imageDataFiles']] <- imgFileCounts
  patientCounts[['imageDataPersons']] <- imgCounts
  patientCounts[['imageDataPersonsPct']] <- (imgCounts / TARGET_PAT_CNT)*100
  patientCounts[['imageDataPersonsDelta']] <- 0 # get_patient_counts_delta('image')
  patientCounts[['waveDataFiles']] <- waveFileCounts
  patientCounts[['waveDataPersons']] <- waveCounts
  patientCounts[['waveDataPersonsPct']] <- (waveCounts / TARGET_PAT_CNT)*100
  patientCounts[['waveDataPersonsDelta']] <- 0 # get_patient_counts_delta('wave')
  patientCounts[['noteDataPersons']] <- noteCounts
  patientCounts[['noteDataFiles']] <- querySql(conn,"SELECT COUNT(*) FROM omopcdm.note")[[1]]
  patientCounts[['noteDataPersonsPct']] <- (noteCounts / TARGET_PAT_CNT)*100
  patientCounts[['noteDataPersonsDelta']] <- 0 # get_patient_counts_delta('note')
  approvedPersons <- 0 # TODO get_approved_persons()
  patientCounts[['allDataApprovedPersons']] <- approvedPersons
  patientCounts[['allDataApprovedFiles']] <- 0 # get_approved_files
  patientCounts[['allDataApprovedPersonsPct']] <- (approvedPersons / TARGET_PAT_CNT)*100
  patientCounts[['allDataApprovedPersonsDelta']] <- 0 # get_patient_counts_delta('approved')
  deidPersons <- 0 # TODO get_deid_persons()
  patientCounts[['allDataApprovedDeidPersons']] <- deidPersons
  patientCounts[['allDataApprovedDeidFiles']] <- 0 # get_approved_deid_files
  patientCounts[['allDataApprovedDeidPersonsPct']] <- (deidPersons / TARGET_PAT_CNT)*100
  patientCounts[['allDataApprovedDeidPersonsDelta']] <- 0 # get_patient_counts_delta('deid')
  
  section1[['PatientCounts']] <- patientCounts
  
  reportSections[['section1']] <- section1
  
  # SECTION 2 - PHI
  
  section2 <- hash()
  phiOverview <- hash()
  
  #phiOverview[['omop']] <- querySql(conn,"select tab, col, phi_prob, uniq_ratio from omopcdm.phi_output WHERE pred_result = 1 order by phi_prob DESC, tab ASC, col ASC LIMIT 20;")
  #phiOverview[['wave']] <- 0 # get_wave_phi()
  #phiOverview[['imag']] <- 0 # get_imag_phi()
  #phiOverview[['note']] <- 0 # get_note_phi()
  
  section2[['phiOverview']] <- phiOverview
  
  reportSections[['section2']] <- section2
  
  # SECTION 3 - Data Quality
  
  section3 <- hash()
  dqdResults <- hash()
  
  dqdResults[['qualityPerCheck']] <- querySql(conn, "select cdm_table_name, category, count(*) AS count_failed FROM omopcdm.dqdashboard_results WHERE failed = '1' GROUP BY cdm_table_name, category ORDER BY 3 DESC;")
  dqdResults[['allResults']] <- querySql(conn, "select * FROM omopcdm.dqdashboard_results;")
  
  section3[['DQDResults']] <- dqdResults
  
  reportSections[['section3']] <- section3
  
  # SECTION 4 - Cohorts and Characterization
  
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
  
  
  disconnect(conn)
  disconnect(connMerge)
  return(reportSections)
}
    