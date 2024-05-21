# @file Helper.R
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
#' @param sqlOnly                          Boolean to determine if Achilles should be fully executed. TRUE = just generate SQL files, don't actually run, FALSE = run Achilles
#' @param databaseName		                 String name of the database name. If blank, CDM_SOURCE table will be queried to try to obtain this.
#' @param databaseDescription              Provide a short description of the database.
#' @param smallCellCount                   To avoid patient identifiability, cells with small counts (<= smallCellCount) are deleted. Set to NULL if you don't want any deletions.
#' @param outputFolder                     Path to store logs and SQL files
#' @param verboseMode                      Boolean to determine if the console will show all execution steps. Default = TRUE
#'
executeQuery <- function(outputFolder,
                         sqlFileName,
                         successMessage,
                         connectionDetails,
                         sqlOnly,
                         cdmDatabaseSchema,
                         vocabDatabaseSchema=NULL,
                         resultsDatabaseSchema=NULL,
                         smallCellCount = 5){
  sql <- SqlRender::loadRenderTranslateSql(sqlFilename = file.path("checks",sqlFileName),
                                           packageName = "CHoRUSReports",
                                           dbms = connectionDetails$dbms,
                                           warnOnMissingParameters = FALSE,
                                           vocabDatabaseSchema = vocabDatabaseSchema,
                                           cdmDatabaseSchema = cdmDatabaseSchema,
                                           resultsDatabaseSchema = resultsDatabaseSchema,
                                           smallCellCount = smallCellCount)

  duration = -1
  result = NULL
  if (sqlOnly) {
    SqlRender::writeSql(sql = sql, targetFile = file.path(outputFolder, sqlFileName))
  } else {

    tryCatch({
      start_time <- Sys.time()
      connection <- DatabaseConnector::connect(connectionDetails = connectionDetails,)
      result<- DatabaseConnector::querySql(connection = connection, sql = sql, errorReportFile = file.path(outputFolder, paste0(tools::file_path_sans_ext(sqlFileName),"Err.txt")))
      duration <- as.numeric(difftime(Sys.time(),start_time), units="secs")
      ParallelLogger::logInfo(paste("> ",successMessage, "in", sprintf("%.2f", duration),"secs"))
    },
    error = function (e) {
      ParallelLogger::logError(paste0("> Failed see ",file.path(outputFolder,paste0(tools::file_path_sans_ext(sqlFileName),"Err.txt"))," for more details"))
    }, finally = {
      DatabaseConnector::disconnect(connection = connection)
      rm(connection)
    })

  }


  return(list(result=result,duration=duration))
}
prettyHr <- function(x) {
  result <- sprintf("%.2f", x)
  result[is.na(x)] <- "NA"
  result <- suppressWarnings(format(as.numeric(result), big.mark=",")) # add thousands separator
  return(result)
}



my_body_add_table <- function (x, value, style = NULL, pos = "after", header = TRUE,
          alignment = NULL, stylenames = table_stylenames(), first_row = TRUE,
          first_column = FALSE, last_row = FALSE, last_column = FALSE,
          no_hband = FALSE, no_vband = TRUE, align = "left")
{
  pt <- officer::prop_table(style = style, layout = table_layout(),
                   width = table_width(), stylenames = stylenames,
                   tcf = table_conditional_formatting(first_row = first_row,
                                                      first_column = first_column, last_row = last_row,
                                                      last_column = last_column, no_hband = no_hband, no_vband = no_vband),
                   align = align)

  # Align left if no alignment is given
  if (is.null(alignment)) {
    alignment <- rep('l', ncol(value))
  }

  # Formatting numeric columns: align right and add thousands separator.
  for (i in 1:ncol(value)) {
    if (is.numeric(value[,i])) {
      value[,i] <- format(value[,i], big.mark=",")
      alignment[i] <- 'r'
    }
  }

  bt <- officer::block_table(x = value, header = header, properties = pt,
                    alignment = alignment)
  xml_elt <- officer::to_wml(bt, add_ns = TRUE, base_document = x)
  officer::body_add_xml(x = x, str = xml_elt, pos = pos)
}


my_source_value_count_section <- function (x, data, table_number, domain, kind) {
  n <- nrow(data$result)
  officer::body_add_par(value = paste0("Table ", table_number, ": ", domain), style = "heading 2") %>%
  if (n == 0) {
    officer::body_add_par(x, paste0("Table ", table_number, " omitted because no ", kind, " ", domain, " were found."), )
  } else{
    officer::body_add_par(x, paste0("All ", n, " ", kind, " ", domain, ". "))
  }

  if (n>0) {
    my_body_add_table(x, value = data$result, style = "CHoRUS")
  }

}

my_table_check <- function(x, data, data_all, table_count, ...) {
  if (table_count == 0) {
    my_body_add_table(x, value = data_all$mappedNothing$result, style = "CHoRUS")
  } else {
    my_body_add_table(x, value = data$result, style = "CHoRUS")
  }
}


recordsCountPlot <- function(results){
  temp <- results %>%
    dplyr::rename(Date=X_CALENDAR_MONTH,Domain=SERIES_NAME, Count=Y_RECORD_COUNT) %>%
    dplyr::mutate(Date=lubridate::parse_date_time(Date, "ym"))
  plot <- ggplot2::ggplot(temp, aes(x = Date, y = Count)) + geom_line(aes(color = Domain))
}

#' Bundles the results in a zip file
#'
#' @description
#' \code{bundleResults} creates a zip file with results in the outputFolder
#' @param outputFolder  Folder to store the results
#' @param databaseId    ID of your database, this will be used as subfolder for the results.
#' @export
bundleResults <- function(outputFolder, databaseId) {
  zipName <- file.path(outputFolder, paste0("Results_CHoRUS_", databaseId, ".zip"))
  files <- list.files(outputFolder, "*.*", full.names = TRUE, recursive = TRUE)
  oldWd <- setwd(outputFolder)
  on.exit(setwd(oldWd), add = TRUE)
  DatabaseConnector::createZipFile(zipFile = zipName, files = files)
  return(zipName)
}

#' Retrieves list of files and metadata from Azure storage containr
#'
#' @description
#' \code{bundleResults} Retrieves list of files and metadata from Azure storage containr
#' @param container_url  Web location of the storage container
#' @param sas    sas access token of the containr
#' @export
get_blob_files <- function(container_url, sas, container_name) {

  blob_endpoint <- paste0("https://", container_url, "/")
  fl_endp_sas <- AzureStor::storage_endpoint(blob_endpoint, sas=sas)
  blob_container <- AzureStor::storage_container(fl_endp_sas, container_name)

  file_list <- AzureStor::list_storage_files(blob_container, recursive=TRUE, info="all")

  return(file_list)
}

