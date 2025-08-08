# @file Helper.R
#
# @author Tufts Medical Center
# @author Jared Houghtaling

#' Helper functions for CHoRUS Reports
#'
#' @description
#' \code{Helper} provides helper functions to create the reports for CHoRUS Data Sites
#'
#' @details
#' \code{Helper} provides helper functions to create the reports for CHoRUS Data Sites
#' @param connectionDetails                 connectionDetails object for DatabaseConnector
#' @param prior_oid                         oid generated during upload of RDS object into postgres
#' @param databaseName                      Name of the data site under review
#' @return                                  An object containing results from the most recent run
#' @export
#'

getPriorReleaseRds <- function(connectionDetails, priorOid, databaseName) {
  pgHost <- strsplit(connectionDetails$server(), "/")[[1]][1]
  rdsFile <- glue::glue("/tmp/prior_{databaseName}.rds")
  retrieveLO <- glue::glue("export PGPASSWORD=\"{connectionDetails$password()}\" && psql -h {pgHost} -d {databaseName} -U postgres -At -c \"\\lo_export {priorOid} '{rdsFile}'\"")
  success <- system(retrieveLO, intern=TRUE)
  prior_results <- readRDS(rdsFile)
  return(prior_results)
}

