# @file ResultsDocumentGeneration
#
# @author Tufts Medical Center
# @author Jared Houghtaling

# This file is part of CHoRUSReports
#
#' Generates the Results Document
#'
#' @description
#' \code{generateResultsDocument} creates a word document with results based on a template
#' @param results             Results object from \code{CHoRUSReports}
#'
#' @param outputFolder        Folder to store the results
#' @param docTemplate         Name of the document template (CHoRUS)
#' @param authors             List of author names to be added in the document
#' @param databaseDescription Description of the database
#' @param databaseName        Name of the database
#' @param databaseId          Id of the database
#' @param smallCellCount      Date with less than this number of patients are removed
#' @param silent              Flag to not create output in the terminal (default = FALSE)

#' @export
generateResultsDocument<- function(results, outputFolder, docTemplate="CHoRUS", authors = "Author Names", databaseDescription, databaseName, databaseId,smallCellCount,silent=FALSE) {

  if (docTemplate=="CHoRUS"){
    docTemplate <- system.file("templates", "Template-CHoRUS.docx", package="CHoRUSReports")
    logo <- system.file("templates", "pics", "CHoRUS-Multicolor.svg", package="CHoRUSReports")
  }

  ## open a new doc from the doctemplate
  doc<-officer::read_docx(path = docTemplate)
  ## add Title Page
  doc<- doc %>%
    officer::body_add_par(value = paste0("CHoRUS report for the ",databaseName," data generating site"), style = "Title") %>%
    officer::body_add_par(value = paste0("Package Version: ", packageVersion("CHoRUSReports")), style = "Centered") %>%
    officer::body_add_par(value = paste0("Date: ", date()), style = "Centered") %>%
    officer::body_add_par(value = paste0("Authors: ", authors), style = "Centered") %>%
    officer::body_add_break()

  ## add Table of content
  doc<-doc %>%
    officer::body_add_par(value = "Table of contents", style = "heading 1") %>%
    officer::body_add_toc(level = 2) %>%
    officer::body_add_break()


    ## add Concept counts
  if (!is.null(results$dataTablesResults)) {
    df_t1 <- results$dataTablesResults$dataTablesCounts$result
    doc<-doc %>%
      officer::body_add_par(value = "OMOP Table Counts", style = "heading 2") %>%
      officer::body_add_par("Counts across the clinical tables") %>%
      my_body_add_table(value = df_t1[order(df_t1$COUNT, decreasing=TRUE),], style = "CHoRUS") %>%
      officer::body_add_par(" ") %>%
      officer::body_add_break()
  }

  vocabResults <-results$vocabularyResults

  drug_rows <- nrow(vocabResults$mappedDrugs$result)
  condition_rows <- nrow(vocabResults$mappedConditions$result)
  meas_rows <- nrow(vocabResults$mappedMeasurements$result)
  obs_rows <- nrow(vocabResults$mappedObservations$result)
  proc_rows <- nrow(vocabResults$mappedProcedures$result)
  device_rows <- nrow(vocabResults$mappedDevices$result)
  detail_rows <- nrow(vocabResults$mappedVisitDetails$result)
  visit_rows <- nrow(vocabResults$mappedVisits$result)

  drug_counts <- sum(vocabResults$mappedDrugs$result$`#RECORDS`)
  condition_counts <- sum(vocabResults$mappedConditions$result$`#RECORDS`)
  meas_counts <- sum(vocabResults$mappedMeasurements$result$`#RECORDS`)
  obs_counts <- sum(vocabResults$mappedObservations$result$`#RECORDS`)
  proc_counts<- sum(vocabResults$mappedProcedures$result$`#RECORDS`)
  device_counts <- sum(vocabResults$mappedDevices$result$`#RECORDS`)
  detail_counts <- sum(vocabResults$mappedVisitDetails$result$`#RECORDS`)
  visit_counts <- sum(vocabResults$mappedVisits$result$`#RECORDS`)

  ## Print counts per domain
  doc<-doc %>%
    officer::body_add_par(value = "Medical Events Captured", style = "heading 1") %>%
    officer::body_add_par(value = "The tables in the section below show concepts captured by the clinical tables") %>%
    officer::body_add_par(value = "Drugs", style = "heading 2") %>%
    officer::body_add_par(paste0("There were a total of ", drug_counts, " Drug Exposures submitted"))
  doc <- doc %>%
    my_table_check(x = doc, data = vocabResults$mappedDrugs, data_all = vocabResults, table_count = drug_rows) %>%
    officer::body_add_break() %>%
    officer::body_add_par(value = "Conditions", style = "heading 2") %>%
    officer::body_add_par(paste0("There were a total of ", condition_counts, " Conditions submitted"))
  doc <- doc %>%
    my_table_check(x = doc, data = vocabResults$mappedConditions, data_all = vocabResults, table_count = condition_rows) %>%
    officer::body_add_break() %>%
    officer::body_add_par(value = "Measurements", style = "heading 2") %>%
    officer::body_add_par(paste0("There were a total of ", meas_counts, " Measurements submitted"))
  doc <- doc %>%
    my_table_check(x = doc, data = vocabResults$mappedMeasurements, data_all = vocabResults, table_count = meas_rows) %>%
    officer::body_add_break() %>%
    officer::body_add_par(value = "Observations", style = "heading 2") %>%
    officer::body_add_par(paste0("There were a total of ", obs_counts, " Observations submitted"))
  doc <- doc %>%
    my_table_check(x = doc, data = vocabResults$mappedObservations, data_all = vocabResults, table_count = obs_rows) %>%
    officer::body_add_break() %>%
    officer::body_add_par(value = "Procedures", style = "heading 2") %>%
    officer::body_add_par(paste0("There were a total of ", proc_counts, " Procedures submitted"))
  doc <- doc %>%
    my_table_check(x = doc, data = vocabResults$mappedProcedures, data_all = vocabResults, table_count = proc_rows) %>%
    officer::body_add_break() %>%
    officer::body_add_par(value = "Devices", style = "heading 2") %>%
    officer::body_add_par(paste0("There were a total of ", device_counts, " Devices submitted"))
  doc <- doc %>%
    my_table_check(x = doc, data = vocabResults$mappedDevices, data_all = vocabResults, table_count = device_rows) %>%
    officer::body_add_break() %>%
    officer::body_add_par(value = "Visit Details", style = "heading 2") %>%
    officer::body_add_par(paste0("There were a total of ", detail_counts, " Visit Details submitted"))
  doc <- doc %>%
    my_table_check(x = doc, data = vocabResults$mappedVisitDetails, data_all = vocabResults, table_count = detail_rows) %>%
    officer::body_add_break() %>%
    officer::body_add_par(value = "Visits", style = "heading 2") %>%
    officer::body_add_par(paste0("There were a total of ", visit_counts, " Visits submitted"))
  doc <- doc %>%
    my_table_check(x = doc, data = vocabResults$mappedVisits, data_all = vocabResults, table_count = visit_rows)

  ## save the doc as a word file
  writeLines(paste0("Saving doc to ",outputFolder,"/",results$databaseId,"-Report.docx"))
  print(doc, target = paste(outputFolder,"/",results$databaseId,"-Report.docx",sep = ""))
}
