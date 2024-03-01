# @file CHoRUSReports
#
# @author Tufts Medical Center
# @author Jared Houghtaling


#' The main CHoRUSReports analyses (for v5.x)
#'
#' @description
#' \code{CHoRUSReports} runs a list of checks as part of the CHoRUS procedure
#'
#' @details
#' \code{CHoRUSReports} runs a list of checks as part of the CHoRUS procedure
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
#' @return                                 An object of type \code{achillesResults} containing details for connecting to the database containing the results
#' @export
CHoRUSReports <- function (connectionDetails,
                             cdmDatabaseSchema,
                             resultsDatabaseSchema = cdmDatabaseSchema,
                             vocabDatabaseSchema = cdmDatabaseSchema,
                             oracleTempSchema = resultsDatabaseSchema,
                             databaseName = "",
                             databaseId = "",
                             databaseDescription = "",
                             smallCellCount = 5,
                             runVocabularyChecks = TRUE,
                             runDataTablesChecks = TRUE,
                             sqlOnly = FALSE,
                             outputFolder = "output",
                             verboseMode = TRUE) {


  # Log execution -----------------------------------------------------------------------------------------------------------------
  ParallelLogger::clearLoggers()
  if(!dir.exists(outputFolder)){dir.create(outputFolder,recursive=T)}

  logFileName <-"log_CHoRUSReports.txt"

  if (verboseMode) {
    appenders <- list(ParallelLogger::createConsoleAppender(),
                      ParallelLogger::createFileAppender(layout = ParallelLogger::layoutParallel,
                                                         fileName = file.path(outputFolder, logFileName)))
  } else {
    appenders <- list(ParallelLogger::createFileAppender(layout = ParallelLogger::layoutParallel,
                                                         fileName = file.path(outputFolder, logFileName)))
  }

  logger <- ParallelLogger::createLogger(name = "CHoRUSReports",
                                         threshold = "INFO",
                                         appenders = appenders)
  ParallelLogger::registerLogger(logger)

  start_time <- Sys.time()

  cdmVersion <- .getCdmVersion(connectionDetails, cdmDatabaseSchema)

  cdmVersion <- as.character(cdmVersion)

  # Check CDM version is valid ---------------------------------------------------------------------------------------------------
  if (compareVersion(a = as.character(cdmVersion), b = "5") < 0) {
    ParallelLogger::logError("Not possible to execute the check, this function is only for v5 and above.")
    ParallelLogger::logError("Is the CDM version available in the cdm_source table?")
    return(NULL)
  }


  # Establish folder paths --------------------------------------------------------------------------------------------------------

  if (!dir.exists(outputFolder)) {
    dir.create(path = outputFolder, recursive = TRUE)
  }

  # Get source name if none provided ----------------------------------------------------------------------------------------------

  if (missing(databaseName) & !sqlOnly) {
    databaseName <- .getDatabaseName(connectionDetails, cdmDatabaseSchema)
  }

  # Logging
  ParallelLogger::logInfo(paste0("Report generation for database ",databaseName, " started (cdm_version=",cdmVersion,")"))

  # run all the checks ------------------------------------------------------------------------------------------------------------
  dataTablesResults <- NULL
  cdmSource<-NULL

  if (runDataTablesChecks) {
    ParallelLogger::logInfo(paste0("Running Data Table Checks"))
    dataTablesResults <- dataTablesChecks(connectionDetails = connectionDetails,
                                  cdmDatabaseSchema = cdmDatabaseSchema,
                                  resultsDatabaseSchema = resultsDatabaseSchema,
                                  outputFolder = outputFolder,
                                  sqlOnly = sqlOnly)
    cdmSource<- .getCdmSource(connectionDetails, cdmDatabaseSchema,sqlOnly,outputFolder)
    temp <- cdmSource
    temp$CDM_RELEASE_DATE <- as.character(cdmSource$CDM_RELEASE_DATE)
    temp$SOURCE_RELEASE_DATE <- as.character(cdmSource$SOURCE_RELEASE_DATE)
    cdmSource <- temp
  }


  vocabularyResults <- NULL
  if (runVocabularyChecks) {
    ParallelLogger::logInfo(paste0("Running Vocabulary Checks"))
    vocabularyResults<-vocabularyChecks(connectionDetails = connectionDetails,
                     cdmDatabaseSchema = cdmDatabaseSchema,
                     vocabDatabaseSchema = vocabDatabaseSchema,
                     resultsDatabaseSchema = resultsDatabaseSchema,
                     smallCellCount = smallCellCount,
                     oracleTempSchema = oracleTempSchema,
                     sqlOnly = sqlOnly,
                     outputFolder = outputFolder)
  }
  packinfo <- NULL
  sys_details <- NULL
  hadesPackageVersions <- NULL
  performanceResults <- NULL
  missingPackages <- NULL
  webAPIversion <- NULL

  duration <- as.numeric(difftime(Sys.time(),start_time), units="mins")
  ParallelLogger::logInfo(paste("Complete CHoRUSReports took ", sprintf("%.2f", duration)," minutes"))
  # save results  ------------------------------------------------------------------------------------------------------------

  results<-list(executionDate = date(),
                executionDuration = as.numeric(difftime(Sys.time(),start_time), units="secs"),
                databaseName = databaseName,
                databaseId = databaseId,
                databaseDescription = databaseDescription,
                vocabularyResults = vocabularyResults,
                dataTablesResults = dataTablesResults,
                packinfo=packinfo,
                hadesPackageVersions = hadesPackageVersions,
                missingPackages = missingPackages,
                performanceResults = performanceResults,
                sys_details= sys_details,
                webAPIversion = webAPIversion,
                cdmSource = cdmSource,
                dms=connectionDetails$dbms)



  saveRDS(results, file.path(outputFolder,"inspection_results.rds"))
  ParallelLogger::logInfo(sprintf("The CHoRUS results have been exported to: %s", outputFolder))
  bundledResultsLocation <- bundleResults(outputFolder, databaseId)
  ParallelLogger::logInfo(paste("All CHoRUS results are bundled for sharing at: ", bundledResultsLocation))
  ParallelLogger::logInfo("Next step: generate and complete the inspection report and share this together with the zip file.")

  duration <- as.numeric(difftime(Sys.time(),start_time), units="secs")
  ParallelLogger::logInfo(paste("CHoRUSReports run took", sprintf("%.2f", duration),"secs"))
  return(results)
}

.getDatabaseName <- function(connectionDetails,
                           cdmDatabaseSchema) {
  sql <- SqlRender::render(sql = "select cdm_source_name from @cdmDatabaseSchema.cdm_source",
                           cdmDatabaseSchema = cdmDatabaseSchema)
  sql <- SqlRender::translate(sql = sql, targetDialect = connectionDetails$dbms)
  connection <- DatabaseConnector::connect(connectionDetails = connectionDetails)
  databaseName <- tryCatch({
    s <- DatabaseConnector::querySql(connection = connection, sql = sql)
    s[1,]
  }, error = function (e) {
    ""
  }, finally = {
    DatabaseConnector::disconnect(connection = connection)
    rm(connection)
  })
  databaseName
}

.getCdmVersion <- function(connectionDetails,
                           cdmDatabaseSchema) {
  sql <- SqlRender::render(sql = "select cdm_version from @cdmDatabaseSchema.cdm_source",
                           cdmDatabaseSchema = cdmDatabaseSchema)
  sql <- SqlRender::translate(sql = sql, targetDialect = connectionDetails$dbms)
  connection <- DatabaseConnector::connect(connectionDetails = connectionDetails)
  cdmVersion <- tryCatch({
    c <- tolower((DatabaseConnector::querySql(connection = connection, sql = sql))[1,])
    gsub(pattern = "v", replacement = "", x = c)
  }, error = function (e) {
    ""
  }, finally = {
    DatabaseConnector::disconnect(connection = connection)
    rm(connection)
  })

  cdmVersion
}

.getCdmSource <- function(connectionDetails,
                           cdmDatabaseSchema,sqlOnly,outputFolder) {
  sql <- SqlRender::loadRenderTranslateSql(sqlFilename = file.path("checks","get_cdm_source_table.sql"),
                                           packageName = "CHoRUSReports",
                                           dbms = connectionDetails$dbms,
                                           warnOnMissingParameters = FALSE,
                                           cdmDatabaseSchema = cdmDatabaseSchema)
  if (sqlOnly) {
    SqlRender::writeSql(sql = sql, targetFile = file.path(outputFolder, "get_cdm_source_table.sql"))
    return(NULL)
  } else {
    tryCatch({
      connection <- DatabaseConnector::connect(connectionDetails = connectionDetails)
      cdmSource<- DatabaseConnector::querySql(connection = connection, sql = sql, errorReportFile = file.path(outputFolder, "vocabulariesError.txt"))
      ParallelLogger::logInfo("> Vocabulary table successfully extracted")
    },
    error = function (e) {
      ParallelLogger::logError(paste0("> Vocabulary table could not be extracted, see ",file.path(outputFolder,"vocabulariesError.txt")," for more details"))
    }, finally = {
      DatabaseConnector::disconnect(connection = connection)
      rm(connection)
    })
    return(cdmSource)
  }
}
