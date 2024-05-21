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

createReportSections <- function (connectionDetails,
                                 cdmDatabaseSchema,
                                 databaseName = "",
                                 databaseId = "",
                                 databaseDescription = "",
                                 smallCellCount = 5,
                                 runVocabularyChecks = TRUE,
                                 runDataTablesChecks = TRUE,
                                 sqlOnly = FALSE,
                                 outputFolder = "output",
                                 verboseMode = TRUE,
                                 accountUrl = "",
                                 accountKey = "",
                                 accountName = "") {
    reportSections <- hash()

    # SECTION 1

    section1 <- hash()
    metadata <- hash()
    overview <- hash()
    patientCounts <- hash()
    differential <- hash()
    allFiles <- get_blob_files(accountUrl, accountKey, accountName)
    allFiles <- allFiles %>% janitor::clean_names() # eliminate hash in names from Azure
    metadata[['deliveryTime']] <- max(allFiles$last_modified) # get delivery time
    metadata[['packetSize']] <- round(sum(allFiles$size)/1000000000, digits=2) # get delivery packet size in gb
    metadata[['numOfPriorDeliveries']] <- 0 # TODO get_prior_deliveries(site)
    metadata[['recentFeedback']] <- c() # TODO get_latest_feedback(site)
    section1[['Metadata']] <- metadata

    dqd_overview <- querySql(conn,"select category,  sum(passed) as passed, sum(failed) as failed FROM omopcdm.dqdashboard_results GROUP BY category ORDER BY 3 desc;")
    overview[['filesDelivered']] <- nrow(allFiles)
    overview[['qualityChecks']] <- dqd_overview
    section1[['Overview']] <- overview

    reportSections[['section1']] <- section1