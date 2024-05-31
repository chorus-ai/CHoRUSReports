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
#' @param connectionDetails   An R object of type \code{connectionDetails} created using the function \code{createConnectionDetails} in the \code{DatabaseConnector} package.
#' @param outputFolder        Folder to store the results
#' @param docTemplate         Name of the document template (CHoRUS)
#' @param authors             List of author names to be added in the document
#' @param databaseDescription Description of the database
#' @param databaseName        Name of the database
#' @param databaseId          Id of the database
#' @param smallCellCount      Date with less than this number of patients are removed
#' @param silent              Flag to not create output in the terminal (default = FALSE)

#' @export
generateResultsDocument<- function(results, connectionDetails, outputFolder, docTemplate="CHoRUS", authors = "Author Names", databaseDescription, databaseName, databaseId,smallCellCount,silent=FALSE) {

  if (docTemplate=="CHoRUS"){
    docTemplate <- system.file("templates", "Template-CHoRUS.docx", package="CHoRUSReports")
    logo <- system.file("templates", "pics", "CHoRUS-Multicolor.svg", package="CHoRUSReports")
  }
  outputFolder <- paste(outputFolder, Sys.Date(), sep="/")
  dir.create(outputFolder, showWarnings = FALSE, recursive = TRUE)
  conn <- connect(connectionDetails)

  ## open a new doc from the doctemplate
  doc<-officer::read_docx(path = docTemplate)
  ## add Title Page
  doc<- doc %>%
    officer::body_add_par(value = paste0("CHoRUS report for the ",toupper(databaseName)," data generating site"), style = "Title") %>%
    officer::body_add_par(value = paste0("Package Version: ", packageVersion("CHoRUSReports")), style = "Centered") %>%
    officer::body_add_par(value = paste0("Date: ", date()), style = "Centered") %>%
    officer::body_add_par(value = paste0("Authors: ", authors), style = "Centered") %>%
    officer::body_add_break()

  ## add Table of contents
  doc<-doc %>%
    officer::body_add_par(value = "Table of contents", style = "heading 1") %>%
    officer::body_add_toc(level = 2) %>%
    officer::body_add_break()


  ## SECTION 1 Table: Metadata
  metric <- c("Delivery Timestamp",
             "Packet Size (GB)",
             "Number of Prior Deliveries",
             "Feedback from Most Recent Delivery"
  )
  metricShort <- c("delivered_at",
                   "packet_size",
                   "num_prior_delivs",
                   "prior_feedback"
  )
  value <- c(glue::glue("{results$section1$Metadata$deliveryTime}"),
               glue::glue("{prettyNum(results$section1$Metadata$packetSize)}"),
               glue::glue("{results$section1$Metadata$numOfPriorDeliveries}"),
               glue::glue("{results$section1$Metadata$recentFeedback}")
               )
  metadataTable <- data.frame(metric,value)
  metadataInsert <- setNames(data.frame(matrix(ncol = 4, nrow = 0)), metricShort)
  metadataInsert[1,] <- value
  ft1 <- flextable::qflextable(metadataTable)
  ft1 <-flextable::set_table_properties(ft1, width = 1, layout = "fixed")
  ft1 <- flextable::bold(ft1, bold = TRUE, part = "header")
  border_v = officer::fp_border(color="gray")
  border_h = officer::fp_border(color="gray")
  ft1<-flextable::border_inner_v(ft1, part="all", border = border_v )
  ft1 <- flextable::fontsize(ft1, size = 9, part = "body")
  ft1 <- flextable::fontsize(ft1, size = 10, part = "header")
  
  ## SECTION 1 Table: High-Level Quantities
  metric <- c("# Files Delivered",
             "DQD Success Percentage",
             "DQD Failed Checks",
             "PHI Issues - OMOP",
             "PHI Issues - Waveform",
             "PHI Issues - Images",
             "PHI Issues - Notes",
             "CHoRUS Quality Checks",
             "DelPhi Capture Percentage"
  )
  
  value <- c(results$section1$Overview$filesDelivered,
               results$section1$Overview$qualityChecks,
               results$section1$Overview$dqdFailures,
               results$section1$Overview$phiIssuesOMOP,
               results$section1$Overview$phiIssuesWAVE,
               results$section1$Overview$phiIssuesIMAG,
               results$section1$Overview$phiIssuesNOTE,
               results$section1$Overview$chorusQC,
               results$section4$DelphiCounts$delphiPercentCapture)
  
  hloTable <- data.frame(metric,value)
  ft2 <- flextable::qflextable(hloTable)
  ft2 <-flextable::set_table_properties(ft2, width = 1, layout = "fixed")
  ft2 <- flextable::bold(ft2, bold = TRUE, part = "header")
  border_v = officer::fp_border(color="gray")
  border_h = officer::fp_border(color="gray")
  ft2<-flextable::border_inner_v(ft2, part="all", border = border_v )
  ft2 <- flextable::fontsize(ft2, size = 9, part = "body")
  ft2 <- flextable::fontsize(ft2, size = 10, part = "header")
  ft2 <- flextable::footnote(ft2,
                            i = 5:7, j = 1,
                            value = flextable::as_paragraph(
                              c(
                                "PHI tools have not been distributed for this modality"
                              )
                            ),
                            ref_symbols = c("a")
  )
  ft2 <- flextable::footnote(ft2,
                             i = 8:9, j = 1,
                             value = flextable::as_paragraph(
                               c(
                                 "CHoRUS-Specific Characterization is Under Development"
                               )
                             ),
                             ref_symbols = c("b")
  )
  ft2 <- flextable::fontsize(ft2, size = 7, part = "footer")
  
  ## SECTION 1 Table: Patient Targets
  category <- c("ALL Data Modes",
             "ANY Data Modes",
             "OMOP Data",
             "Imaging Data",
             "Waveform Data",
             "Note Data",
             "Approved",
             "Approved & Deidentified"
  )
  persCnt <- c(results$section1$PatientCounts$allDataPersons,
                    results$section1$PatientCounts$anyDataPersons,
                    results$section1$PatientCounts$omopDataPersons,
                    results$section1$PatientCounts$imageDataPersons,
                    results$section1$PatientCounts$waveDataPersons,
                    results$section1$PatientCounts$noteDataPersons,
                    results$section1$PatientCounts$allDataApprovedPersons,
                    results$section1$PatientCounts$allDataApprovedDeidPersons)
  
  fileCnt   <-  c(results$section1$PatientCounts$allDataFiles,
                  results$section1$PatientCounts$anyDataFiles,
                  results$section1$PatientCounts$omopDataFiles,
                  results$section1$PatientCounts$imageDataFiles,
                  results$section1$PatientCounts$waveDataFiles,
                  results$section1$PatientCounts$noteDataFiles,
                  results$section1$PatientCounts$allDataApprovedFiles,
                  results$section1$PatientCounts$allDataApprovedDeidFiles)
  
  targPct    <- c(results$section1$PatientCounts$allDataPersonsPct,
                        results$section1$PatientCounts$anyDataPersonsPct,
                        results$section1$PatientCounts$omopDataPersonsPct,
                        results$section1$PatientCounts$imageDataPersonsPct,
                        results$section1$PatientCounts$waveDataPersonsPct,
                        results$section1$PatientCounts$noteDataPersonsPct,
                        results$section1$PatientCounts$allDataApprovedPersonsPct,
                        results$section1$PatientCounts$allDataApprovedDeidPersonsPct)
  
  persDelta    <- c(results$section1$PatientCounts$allDataPersonsDelta,
                      results$section1$PatientCounts$anyDataPersonsDelta,
                      results$section1$PatientCounts$omopDataPersonsDelta,
                      results$section1$PatientCounts$imageDataPersonsDelta,
                      results$section1$PatientCounts$waveDataPersonsDelta,
                      results$section1$PatientCounts$noteDataPersonsDelta,
                      results$section1$PatientCounts$allDataApprovedPersonsDelta,
                      results$section1$PatientCounts$allDataApprovedDeidPersonsDelta)
  
  pcTable <- data.frame(category,fileCnt)
  pcTable["persCnt"] <- persCnt
  pcTable["persDelta"] <- persDelta
  pcTable["targPct"] <- targPct
  
  ft3 <- flextable::qflextable(pcTable)
  ft3 <-flextable::set_table_properties(ft3, width = 1, layout = "fixed")
  ft3 <- flextable::bold(ft3, bold = TRUE, part = "header")
  border_v = officer::fp_border(color="gray")
  border_h = officer::fp_border(color="gray")
  ft3<-flextable::border_inner_v(ft3, part="all", border = border_v )
  ft3 <- flextable::fontsize(ft3, size = 9, part = "body")
  ft3 <- flextable::fontsize(ft3, size = 10, part = "header")
  ft3 <- flextable::footnote(ft3,
                             i = 6:8, j = 1,
                             value = flextable::as_paragraph(
                               c(
                                 "Notes have not yet been ingested due to PHI concerns with data deliveries from several sites",
                                 "The approval process has not yet been defined",
                                 "The deidentification process has not yet been defined"
                               )
                             ),
                             ref_symbols = c("a", "b", "c")
  )
  ft3 <- flextable::fontsize(ft3, size = 7, part = "footer")
  
  options(warn=-1)
  if (nrow(results$section2$phiOverview$omop) == 20){
    results$section2$phiOverview$omop[nrow(results$section2$phiOverview$omop) + 1,] = c("TRUNCATED", "TRUNCATED", "TRUNCATED", "TRUNCATED")
  }
  ft4 <- flextable::qflextable(results$section2$phiOverview$omop)
  options(warn=0)
  ft4 <- flextable::set_table_properties(ft4, width = 1, layout = "autofit")
  ft4 <- flextable::theme_zebra(ft4)
  ft4 <- flextable::fontsize(ft4, size = 8)
  ft4 <- flextable::fontsize(ft4, size = 8, part = "header")
  ft4 <- flextable::footnote(ft4,
                             i = 1, j = 3,
                             value = flextable::as_paragraph(
                               c(
                                 "Note that the PHI tool has not yet been trained on OMOP-shaped data"
                               )
                             ),
                             ref_symbols = c("a"),
                             part = "header"
  )
  ft4 <- flextable::fontsize(ft4, size = 7, part = "footer")
  
  ft5 <- flextable::qflextable(results$section3$DQDResults$qualityPerCheck)
  ft5 <- flextable::set_table_properties(ft5, width = 1, layout = "autofit")
  ft5 <- flextable::theme_zebra(ft5)
  ft5 <- flextable::fontsize(ft5, size = 8)
  ft5 <- flextable::fontsize(ft5, size = 8, part = "header")
  
  ft6 <- flextable::qflextable(results$section4$CohortCounts$topInNetwork)
  ft6 <- flextable::set_table_properties(ft6, width = 1, layout = "autofit")
  ft6 <- flextable::theme_zebra(ft6)
  ft6 <- flextable::fontsize(ft6, size = 8)
  ft6 <- flextable::fontsize(ft6, size = 8, part = "header")
  
  ft7 <- flextable::qflextable(results$section4$CohortCounts$topInSource)
  ft7 <- flextable::set_table_properties(ft7, width = 1, layout = "autofit")
  ft7 <- flextable::theme_zebra(ft7)
  ft7 <- flextable::fontsize(ft7, size = 8)
  ft7 <- flextable::fontsize(ft7, size = 8, part = "header")
  
  ft8 <- flextable::qflextable(results$section4$DelphiCounts$delphiCountsAll[1:25,])
  ft8 <- flextable::set_table_properties(ft8, width = 1, layout = "autofit")
  ft8 <- flextable::theme_zebra(ft8)
  ft8 <- flextable::fontsize(ft8, size = 8)
  ft8 <- flextable::fontsize(ft8, size = 8, part = "header")
  ft8 <- flextable::colformat_int(ft8, big.mark = "") 
  
  ft9 <- flextable::qflextable(results$section4$DelphiCounts$delphiGrouped)
  ft9 <- flextable::set_table_properties(ft9, width = 1, layout = "autofit")
  ft9 <- flextable::theme_zebra(ft9)
  ft9 <- flextable::fontsize(ft9, size = 8)
  ft9 <- flextable::fontsize(ft9, size = 8, part = "header")
  
  
  doc<-doc %>%
    officer::body_add_par(value = "General Delivery Information", style = "heading 1") %>%
    
    officer::body_add_par(value = "The goal of the feedback report is to provide insight into the completeness, transparency and quality of the data transformation processes and the readiness of the data site to participate in CHoRUS research studies.") %>%
    
    officer::body_add_par(value = "Metadata", style = "heading 2") %>%
    
    flextable::body_add_flextable(value = ft1, align = "left") %>%
    
    officer::body_add_break() %>%
    
    officer::body_add_par(value = "High-Level Characterization", style = "heading 2") %>%
    
    officer::body_add_par(value = "The table below includes output counts from various PHI and quality checks executed against the data delivery during the ingestion process") %>%
    
    flextable::body_add_flextable(value = ft2, align = "left") %>%
    
    officer::body_add_break() %>%
    
    officer::body_add_par(value = "Delivery Targets", style = "heading 2") %>%
    
    flextable::body_add_flextable(value = ft3, align = "left") %>%
  
    officer::body_add_break()

  doc<-doc %>%
    officer::body_add_par(value = "PHI Checks", style = "heading 1") %>%
    
    officer::body_add_par(value = "This section of the report details various checks that were executed against the data delivery. If you'd like to learn more about these checks, please see the [Privacy SOP] documentation") %>%
    
    officer::body_add_par(value = "OMOP-Based PHI Checks", style = "heading 2") %>%
    
    officer::body_add_par(value = "The table below shows checks, executed by the privacy_check_tool, that were deemed to have failed (pred_result = 1) based on the trained model.") %>%
    
    flextable::body_add_flextable(value = ft4, align = "left") %>%
    
    officer::body_add_break()
  
  doc<-doc %>%
    officer::body_add_par(value = "DQD Checks", style = "heading 1") %>%
    
    officer::body_add_par(value = "This section of the report provides a summary of failed checks identified by the data quality dashboard. For more detailed information, please refer to the complete csv-based DQD output in this report packet. ") %>%
    
    officer::body_add_par(value = "Standard DQD Check Failures", style = "heading 2") %>%

    flextable::body_add_flextable(value = ft5, align = "left") %>%
  
    officer::body_add_break()
  
  doc<-doc %>%
    officer::body_add_par(value = "Data Characterization", style = "heading 1") %>%
    
    officer::body_add_par(value = "This section of the report details which concepts from the DelPhi process were captured in the dataset. Note that the most-frequent concepts table shows only the top 25 concepts ordered by frequency, and the rest of the table can be found in the results packet.") %>%
    
    officer::body_add_par(value = glue::glue("Most frequent DelPhi concepts in the {databaseName} dataset"), style = "heading 2") %>%
    
    flextable::body_add_flextable(value = ft8, align = "left") %>%
    
    officer::body_add_break() %>%
    
    officer::body_add_par(value = glue::glue("Per-Domain DelPhi concept capture in the {databaseName} dataset"), style = "heading 2") %>%
    
    officer::body_add_par(value = "This section details the capture of DelPhi concepts by OMOP domain") %>%
    
    flextable::body_add_flextable(value = ft9, align = "left") %>%
    
    officer::body_add_break() %>%
    
    officer::body_add_par(value = "This section of the report provides information about how many patients in a site's data set can be captured by cohort definitions from the OHDSI PhenotypeLibrary. ") %>%
    
    officer::body_add_par(value = "Most populous cohorts in the MERGE dataset", style = "heading 2") %>%
    
    flextable::body_add_flextable(value = ft6, align = "left") %>%
    
    officer::body_add_break() %>%
  
    officer::body_add_par(value = glue::glue("Most populous cohorts in {databaseName} dataset"), style = "heading 2") %>%
    
    flextable::body_add_flextable(value = ft7, align = "left")
  

  ## store output of report creation process
  wordFile <- paste0(databaseName,"-Report-", Sys.Date(), ".docx")
  wordPath <- paste0(outputFolder,"/",wordFile)
  
  pdfFile <- paste0(databaseName,"-Report-", Sys.Date(), ".pdf")
  pdfPath <- paste0(outputFolder,"/",pdfFile)
  
  writeLines(paste0("Saving document to ",wordFile," and ", pdfFile))
  print(doc, target = paste(outputFolder,"/",wordFile,sep = ""))
  
  Sys.unsetenv("LD_LIBRARY_PATH") # Fix for strange rstudio behavior -> https://github.com/rstudio/rstudio/issues/8539#issuecomment-1239094139
  cmd_ <- sprintf('export HOME=/tmp && /usr/lib/libreoffice/program/soffice.bin --headless --convert-to soffice --convert-to pdf:draw_pdf_Export:{"SelectPdfVersion":{"type":"long","value":"17"}} %s --outdir %s', wordPath, outputFolder)
  system(cmd_, ignore.stdout = TRUE)
  
  rdsFile <- paste0(outputFolder, "/", databaseName, "-results.rds")
  saveRDS(results, file = rdsFile)
  write.csv(results$section3$DQDResults$allResults, file = paste0(outputFolder, "/", databaseName, "-dqd-results.csv") )
  write.csv(results$section4$DelphiCounts$delphiCountsAll, file = paste0(outputFolder, "/", databaseName, "-delphi-counts.csv") )
  
  # Upload results object to postgres instance and get oid
  pgHost <- strsplit(connectionDetails$server(), "/")[[1]][1]
  sendLO <- glue::glue("export PGPASSWORD=\"{connectionDetails$password()}\" && psql -h {pgHost} -U postgres -At -c \"\\lo_import '{rdsFile}'\" | tail -1 | cut -d \" \" -f 2")
  myoid <- system(sendLO, intern=TRUE)
  
  dbWriteTable(conn, "public.release_tmp", metadataInsert, overwrite = TRUE)
  sqlInsertCheck <- glue::glue("
  INSERT INTO public.allreleases
  SELECT delivered_at::timestamp,
         packet_size::text,
         num_prior_delivs::integer,
         prior_feedback::text,
         {myoid}
  FROM public.release_tmp ON CONFLICT DO NOTHING;
  DROP TABLE public.release_tmp;
  ")
  executeSql(conn, sqlInsertCheck, progressBar = FALSE, reportOverallTime = FALSE)
  disconnect(conn)
}
