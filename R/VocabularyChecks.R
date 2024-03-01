# @file VocabularyChecks
#
# @author Tufts Medical Center
# @author Jared Houghtaling

#' The vocabulary checks (for v5.x)
#'
#' @description
#' \code{CHoRUSReports} runs a list of checks on the vocabulary as part of the CHoRUS procedure
#'
#' @details
#' \code{CHoRUSReports} runs a list of checks on the vocabulary as part of the CHoRUS procedure
#'
#' @param connectionDetails                An R object of type \code{connectionDetails} created using the function \code{createConnectionDetails} in the \code{DatabaseConnector} package.
#' @param cdmDatabaseSchema    	           Fully qualified name of database schema that contains OMOP CDM schema.
#'                                         On SQL Server, this should specifiy both the database and the schema, so for example, on SQL Server, 'cdm_instance.dbo'.
#' @param resultsDatabaseSchema		         Fully qualified name of database schema that we can write final results to. Default is cdmDatabaseSchema.
#'                                         On SQL Server, this should specifiy both the database and the schema, so for example, on SQL Server, 'cdm_results.dbo'.
#' @param vocabDatabaseSchema		           String name of database schema that contains OMOP Vocabulary. Default is cdmDatabaseSchema. On SQL Server, this should specifiy both the database and the schema, so for example 'results.dbo'.
#' @param oracleTempSchema                 For Oracle only: the name of the database schema where you want all temporary tables to be managed. Requires create/insert permissions to this database.
#' @param smallCellCount                   To avoid patient identifiability, cells with small counts (<= smallCellCount) are deleted. Set to NULL if you don't want any deletions.
#' @param sourceName		                   String name of the data source name. If blank, CDM_SOURCE table will be queried to try to obtain this.
#' @param sqlOnly                          Boolean to determine if Achilles should be fully executed. TRUE = just generate SQL files, don't actually run, FALSE = run Achilles
#' @param outputFolder                     Path to store logs and SQL files
#' @param verboseMode                      Boolean to determine if the console will show all execution steps. Default = TRUE
#' @return                                 An object of type \code{achillesResults} containing details for connecting to the database containing the results
#' @export
vocabularyChecks <- function (connectionDetails,
                           cdmDatabaseSchema,
                           resultsDatabaseSchema = cdmDatabaseSchema,
                           vocabDatabaseSchema = cdmDatabaseSchema,
                           oracleTempSchema = resultsDatabaseSchema,
                           smallCellCount = 5,
                           sourceName = "",
                           sqlOnly = FALSE,
                           outputFolder = "output",
                           verboseMode = TRUE) {

  mappedDrugs<- executeQuery(outputFolder,"mapped_drugs.sql", "Mapped drugs query executed successfully", connectionDetails, sqlOnly, cdmDatabaseSchema, vocabDatabaseSchema, resultsDatabaseSchema, smallCellCount)
  mappedConditions<- executeQuery(outputFolder,"mapped_conditions.sql", "Mapped conditions query executed successfully",  connectionDetails, sqlOnly, cdmDatabaseSchema, vocabDatabaseSchema, resultsDatabaseSchema, smallCellCount)
  mappedMeasurements<- executeQuery(outputFolder,"mapped_measurements.sql", "Mapped measurements query executed successfully",  connectionDetails, sqlOnly, cdmDatabaseSchema, vocabDatabaseSchema, resultsDatabaseSchema, smallCellCount)
  mappedObservations<- executeQuery(outputFolder,"mapped_observations.sql", "Mapped observations query executed successfully",  connectionDetails, sqlOnly, cdmDatabaseSchema, vocabDatabaseSchema, resultsDatabaseSchema, smallCellCount)
  mappedProcedures<- executeQuery(outputFolder,"mapped_procedures.sql", "Mapped procedures query executed successfully",  connectionDetails, sqlOnly, cdmDatabaseSchema, vocabDatabaseSchema, resultsDatabaseSchema, smallCellCount)
  mappedDevices<- executeQuery(outputFolder,"mapped_devices.sql", "Mapped devices query executed successfully",  connectionDetails, sqlOnly, cdmDatabaseSchema, vocabDatabaseSchema, resultsDatabaseSchema, smallCellCount)
  mappedVisits<- executeQuery(outputFolder,"mapped_visits.sql", "Mapped visits query executed successfully",  connectionDetails, sqlOnly, cdmDatabaseSchema, vocabDatabaseSchema, resultsDatabaseSchema, smallCellCount)
  mappedVisitDetails<- executeQuery(outputFolder,"mapped_visit_details.sql", "Mapped visit details query executed successfully",  connectionDetails, sqlOnly, cdmDatabaseSchema, vocabDatabaseSchema, resultsDatabaseSchema, smallCellCount)
  mappedNothing<- executeQuery(outputFolder,"mapped_nothing.sql", "Mapped nothing executed successfully",  connectionDetails, sqlOnly, cdmDatabaseSchema, vocabDatabaseSchema, resultsDatabaseSchema, smallCellCount)

  results <- list(mappedDrugs=mappedDrugs,
                  mappedConditions=mappedConditions,
                  mappedMeasurements=mappedMeasurements,
                  mappedObservations=mappedObservations,
                  mappedProcedures=mappedProcedures,
                  mappedDevices=mappedDevices,
                  mappedVisits=mappedVisits,
                  mappedVisitDetails=mappedVisitDetails,
                  mappedNothing=mappedNothing)
  return(results)
}



