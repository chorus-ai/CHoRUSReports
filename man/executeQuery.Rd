% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/Helper.R
\name{executeQuery}
\alias{executeQuery}
\title{The vocabulary checks (for v5.x)}
\usage{
executeQuery(
  outputFolder,
  sqlFileName,
  successMessage,
  connectionDetails,
  sqlOnly,
  cdmDatabaseSchema,
  vocabDatabaseSchema = NULL,
  resultsDatabaseSchema = NULL,
  smallCellCount = 5
)
}
\arguments{
\item{outputFolder}{Path to store logs and SQL files}

\item{connectionDetails}{An R object of type \code{connectionDetails} created using the function \code{createConnectionDetails} in the \code{DatabaseConnector} package.}

\item{sqlOnly}{Boolean to determine if Achilles should be fully executed. TRUE = just generate SQL files, don't actually run, FALSE = run Achilles}

\item{cdmDatabaseSchema}{Fully qualified name of database schema that contains OMOP CDM schema.
On SQL Server, this should specifiy both the database and the schema, so for example, on SQL Server, 'cdm_instance.dbo'.}

\item{vocabDatabaseSchema}{String name of database schema that contains OMOP Vocabulary. Default is cdmDatabaseSchema. On SQL Server, this should specifiy both the database and the schema, so for example 'results.dbo'.}

\item{resultsDatabaseSchema}{Fully qualified name of database schema that we can write final results to. Default is cdmDatabaseSchema.
On SQL Server, this should specifiy both the database and the schema, so for example, on SQL Server, 'cdm_results.dbo'.}

\item{smallCellCount}{To avoid patient identifiability, cells with small counts (<= smallCellCount) are deleted. Set to NULL if you don't want any deletions.}

\item{databaseName}{String name of the database name. If blank, CDM_SOURCE table will be queried to try to obtain this.}

\item{databaseDescription}{Provide a short description of the database.}

\item{verboseMode}{Boolean to determine if the console will show all execution steps. Default = TRUE}
}
\description{
\code{CHoRUSReports} runs a list of checks on the vocabulary as part of the CHoRUS procedure
}
\details{
\code{CHoRUSReports} runs a list of checks on the vocabulary as part of the CHoRUS procedure
}
