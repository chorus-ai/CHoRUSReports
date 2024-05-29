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
#' @param cdmDatabaseSchema    	           Fully qualified name of database schema that contains OMOP CDM schema.
#'                                         On SQL Server, this should specifiy both the database and the schema, so for example, on SQL Server, 'cdm_instance.dbo'.
#' @param resultsDatabaseSchema		         Fully qualified name of database schema that we can write final results to. Default is cdmDatabaseSchema.
#'                                         On SQL Server, this should specifiy both the database and the schema, so for example, on SQL Server, 'cdm_results.dbo'.
#' @param vocabDatabaseSchema		           String name of database schema that contains OMOP Vocabulary. Default is cdmDatabaseSchema. On SQL Server, this should specifiy both the database and the schema, so for example 'results.dbo'.
#' @param oracleTempSchema                 For Oracle only: the name of the database schema where you want all temporary tables to be managed. Requires create/insert permissions to this database.
#' @param databaseId                       ID of your database, this will be used as subfolder for the results.
#' @param databaseName		                 String name of the database name. If blank, CDM_SOURCE table will be queried to try to obtain this.
#' @param databaseDescription              Provide a short description of the database.
#' @param smallCellCount                   To avoid patient identifiability, cells with small counts (<= smallCellCount) are deleted. Set to NULL if you don't want any deletions.
#' @param runVocabularyChecks              Boolean to determine if vocabulary checks need to be run. Default = TRUE
#' @param runDataTablesChecks              Boolean to determine if table checks need to be run. Default = TRUE
#' @param sqlOnly                          Boolean to determine if Achilles should be fully executed. TRUE = just generate SQL files, don't actually run, FALSE = run Achilles
#' @param outputFolder                     Path to store logs and SQL files
#' @param verboseMode                      Boolean to determine if the console will show all execution steps. Default = TRUE
#' @param accountUrl                       Web location of Azure storage account
#' @param accountKey                       SAS key for the Azure storage account
#' @param accountName                      Name of the blob container in question
#' @return                                 An object of type \code{achillesResults} containing details for connecting to the database containing the results
#' @export

createReportSections <- function  (connectionDetails,
                                   connectionDetailsMerge,
                                   cdmDatabaseSchema,
                                   databaseName = "",
                                   databaseId = "",
                                   smallCellCount = 5,
                                   sqlOnly = FALSE,
                                   outputFolder = "output",
                                   verboseMode = TRUE,
                                   accountUrl = "",
                                   accountKey = "",
                                   containerName = "",
                                   depth = 3) {
  reportSections <- hash()
  TARGET_PAT_CNT <- 8000
  conn <- connect(connectionDetails)
  connMerge <- connect(connectionDetailsMerge)
  
  # SECTION 1 - Metadata & Overviews
  
  section1 <- hash()
  metadata <- hash()
  overview <- hash()
  patientCounts <- hash()
  differential <- hash()
  allFiles <- list_blobs(accountUrl, accountKey, containerName)
  allFiles <- allFiles %>% janitor::clean_names() # eliminate hash in names from Azure
  dbWriteTable(conn, "public.allFiles", allFiles, overwrite = TRUE)
  sqlAllfiles <- glue::glue("
    ALTER TABLE public.allfiles ADD COLUMN person text;
    ALTER TABLE public.allfiles ADD COLUMN modality text;
    ALTER TABLE public.allfiles ADD COLUMN filename text;
    ALTER TABLE public.allfiles ADD COLUMN extension text;
    
    UPDATE public.allfiles
    SET person = NULLIF(split_part(name, '/', {depth}-2), ''),
        modality = NULLIF(split_part(name, '/', {depth}-1), ''),
        filename = NULLIF(split_part(name, '/', {depth}), '')
    WHERE name IS NOT NULL;
    
    UPDATE public.allfiles
    SET extension = split_part(filename, '.', -1)
    WHERE filename IS NOT NULL;
  ")
  
  executeSql(conn, sqlAllfiles, progressBar = FALSE, reportOverallTime = FALSE)
  
  metadata[['deliveryTime']] <- max(allFiles$last_modified) # get delivery time
  metadata[['packetSize']] <- querySql(conn,"select sum(size)/1000000000 FROM public.allfiles;")[[1]] # get delivery packet size in gb
  metadata[['numOfPriorDeliveries']] <- 0 # TODO get_prior_deliveries(databaseName)
  metadata[['recentFeedback']] <- c("NO FEEDBACK YET") # TODO get_latest_feedback(databaseName)
  section1[['Metadata']] <- metadata
  
  dqdOverview <- querySql(conn,"select  
                            round(sum(passed)/(sum(passed) + sum(failed))::numeric * 100, 2) as pcnt
                            FROM omopcdm.dqdashboard_results ;")[[1]]
  overview[['filesDelivered']] <- querySql(conn,"select count(*) FROM public.allfiles WHERE extension IS NOT NULL;")[[1]]
  overview[['qualityChecks']] <- dqdOverview
  overview[['dqdFailures']]   <- querySql(conn, "select count(*) FROM omopcdm.dqdashboard_results WHERE failed = '1';")[[1]]
  overview[['phiIssuesOMOP']] <- querySql(conn,"select count(*) from omopcdm.phi_output WHERE pred_result = '1';")[[1]]
  overview[['phiIssuesWAVE']] <- "TBD"
  overview[['phiIssuesIMAG']] <- "TBD"
  overview[['phiIssuesNOTE']] <- "TBD"
  overview[['chorusQC']] <- 'TBD' # TODO get_chorus_qc(databaseName)
  overview[['chorusChars']] <- 'TBD'
  section1[['Overview']] <- overview
  
  
  
  waveCounts <- querySql(conn,glue::glue("select count(DISTINCT person) from public.allfiles WHERE lower(modality) LIKE 'wave%' AND extension IS NOT NULL;"))[[1]]
  waveFileCounts <- querySql(conn,glue::glue("select count(*) from public.allfiles WHERE lower(modality) LIKE 'wave%' AND extension IS NOT NULL;"))[[1]]
  imgCounts <- querySql(conn,"select count(DISTINCT person) from public.allfiles WHERE lower(modality) LIKE 'imag%' AND extension IS NOT NULL;")[[1]]
  imgFileCounts <- querySql(conn,"select count(*) from public.allfiles WHERE lower(modality) LIKE 'imag%' AND extension IS NOT NULL;")[[1]]
  omopCounts <- querySql(conn,"select count(DISTINCT person) from public.allfiles WHERE lower(modality) LIKE 'omop%' AND extension IS NOT NULL;")[[1]]
  omopFileCounts <- querySql(conn,"select count(*) from public.allfiles WHERE (lower(modality) LIKE 'omop%' OR lower(person) = 'omop') AND extension IS NOT NULL;")[[1]]
  if (identical(omopCounts,integer(0))) {
    omopCounts <- querySql(conn,"SELECT COUNT(DISTINCT person_id) FROM omopcdm.person")[[1]]
  }
  noteCounts <- querySql(conn,"SELECT COUNT(DISTINCT person_id) FROM omopcdm.note")[[1]]
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
  
  phiOverview[['omop']] <- querySql(conn,"select tab, col, phi_prob, uniq_ratio from omopcdm.phi_output WHERE pred_result = 1 order by phi_prob DESC, tab ASC, col ASC LIMIT 20;")
  phiOverview[['wave']] <- 0 # get_wave_phi()
  phiOverview[['imag']] <- 0 # get_imag_phi()
  phiOverview[['note']] <- 0 # get_note_phi()
  
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
  
  cohortsCounts[['topInNetwork']] <- querySql(connMerge,glue::glue("select description, cnt_merge, percent_merge, cnt_{databaseName} from omopcdm.cohort_reports ORDER BY cnt_merge DESC limit 20;"))
  cohortsCounts[['topInSource']] <- querySql(connMerge,glue::glue("select description, cnt_merge, percent_merge, cnt_{databaseName} from omopcdm.cohort_reports ORDER BY cnt_{databaseName} DESC limit 20;"))
  
  section4[['CohortCounts']] <- cohortsCounts
  reportSections[['section4']] <- section4
  
  
  disconnect(conn)
  disconnect(connMerge)
  return(reportSections)
}
    