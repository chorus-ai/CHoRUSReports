% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/ResultsDocumentGeneration.R
\name{generateResultsDocument}
\alias{generateResultsDocument}
\title{Generates the Results Document}
\usage{
generateResultsDocument(
  results,
  outputFolder,
  docTemplate = "CHoRUS",
  authors = "Author Names",
  databaseDescription,
  databaseName,
  databaseId,
  smallCellCount,
  silent = FALSE
)
}
\arguments{
\item{results}{Results object from \code{CHoRUSReports}}

\item{outputFolder}{Folder to store the results}

\item{docTemplate}{Name of the document template (CHoRUS)}

\item{authors}{List of author names to be added in the document}

\item{databaseDescription}{Description of the database}

\item{databaseName}{Name of the database}

\item{databaseId}{Id of the database}

\item{smallCellCount}{Date with less than this number of patients are removed}

\item{silent}{Flag to not create output in the terminal (default = FALSE)}
}
\description{
\code{generateResultsDocument} creates a word document with results based on a template
}
