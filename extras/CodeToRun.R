# *******************************************************
# -----------------INSTRUCTIONS -------------------------
# *******************************************************
#
#-----------------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------
# This CodeToRun.R is provided as an example of how to run this package.
# Below you will find 2 sections: the 1st is for installing the dependencies
# required to run the package and the 2nd for running the package.
#
# The code below makes use of R environment variables (denoted by "Sys.getenv(<setting>)") to
# allow for protection of sensitive information. If you'd like to use R environment variables stored
# in an external file, this can be done by creating an .Renviron file in the root of the folder
# where you have cloned this code. For more information on setting environment variables please refer to:
# https://stat.ethz.ch/R-manual/R-devel/library/base/html/readRenviron.html
#
# Below is an example .Renviron file's contents.
# Remove the "#" below as these too are interpreted as comments in the .Renviron file.
#
#    DBMS = "postgresql"
#    DB_SERVER = "database.server.com"
#    DB_PORT = 5432
#    DB_USER = "database_user_name_goes_here"
#    DB_PASSWORD = "your_secret_password"
#    PATH_TO_DRIVER = "C:/path_to_jdbc_driver"
#
# The following describes the settings
#    DBMS, DB_SERVER, DB_PORT, DB_USER, DB_PASSWORD := These are the details used to connect
#    to your database server. For more information on how these are set, please refer to:
#    http://ohdsi.github.io/DatabaseConnector/
#
# Once you have established an .Renviron file, you must restart your R session for R to pick up these new
# variables.
#
# In section 2 below, you will also need to update the code to use your site specific values. Please scroll
# down for specific instructions.
#-----------------------------------------------------------------------------------------------
#
#
# *******************************************************
# SECTION 1: Make sure to install all dependencies (not needed if already done) -------------------------------
# *******************************************************
#
# Prevents errors due to packages being built for other R versions:
Sys.setenv("R_REMOTES_NO_ERRORS_FROM_WARNINGS" = TRUE)
#
# First, it probably is best to make sure you are up-to-date on all existing packages.
# Important: This code is best run in R, not RStudio, as RStudio may have some libraries
# (like 'rlang') in use.
#update.packages(ask = "graphics")

# When asked to update packages, select '1' ('update all') (could be multiple times)
# When asked whether to install from source, select 'No' (could be multiple times)
#install.packages("devtools")
#devtools::install_github("chorus-ai/CHoRUSReports")

# *******************************************************
# SECTION 2: Set Local Details
# *******************************************************

#library(CHoRUSReports)
library(officer)
library(magrittr)
library(DatabaseConnector)
library(ggplot2)
library(ggrepel)
library(dplyr)
library(stringr)
library(patchwork)
library(hash)

site_list = c('columbia',
              'duke',
              'emory',
              'mgh',
              'mit',
              'mayo',
              'nationwide',
              'ucla',
              #'ucsf',
              'florida',
              'pittsburgh',
              'seattle',
              'virginia',
              'tufts'
)

# *******************************************************
# SECTION 3: Run the report generation process
# *******************************************************

for (site in site_list) {

  print(paste0("Generating feedback report for ", site))
  # Author details
  authors <-"CHoRUS Standards Team" # used on the title page

  # Details specific to the database:
  databaseId <- site
  databaseName <- site
  databaseDescription <- paste0("Data delivery provided by ", site)

  # For Oracle: define a schema that can be used to emulate temp tables:
  oracleTempSchema <- NULL

  # Details for connecting to the CDM and storing the results
  outputFolder <- file.path(getwd(), "chorusreports",databaseId)
  cdmDatabaseSchema <- "omopcdm"
  resultsDatabaseSchema <- "omopcdm" # Make sure the Achilles results are in this schema!
  vocabDatabaseSchema <- "omopcdm"

  # All results smaller than this value are removed from the results.
  smallCellCount <- 10

  outputFolder <- file.path(getwd(), "chorusreports",databaseId)

  verboseMode <- TRUE


  connectionDetails <- DatabaseConnector::createConnectionDetails(dbms='postgresql',
                                                                  server=paste0('<DBHOST>', databaseName),
                                                                  user='postgres',
                                                                  password='<DBPASSWORD>',
                                                                  pathToDriver = '/opt/jdbc-drivers/')

  connectionDetailsMerge <- DatabaseConnector::createConnectionDetails(dbms='postgresql',
                                                                       server='<DBHOST>',
                                                                       user='postgres',
                                                                       password='<DBPASSWORD>',
                                                                       pathToDriver = '/opt/jdbc-drivers/')

  connectionDetailsOhdsi <- DatabaseConnector::createConnectionDetails(dbms='postgresql',
                                                                       server='<DBHOST>',
                                                                       user='postgres',
                                                                       password='<DBPASSWORD>',
                                                                       pathToDriver = '/opt/jdbc-drivers/')
  print("Making CDM connection for table characterization...")
  cdm <- CDMConnector::cdmFromCon(
    con = DBI::dbConnect(
      drv = RPostgres::Postgres(), dbname =databaseName,
      host = '<DBHOST>', port = 5432,
      user = 'postgres', password = '<DBPASSWORD>'
    ),
    cdmSchema = "omopcdm",
    writeSchema = "public",
    writePrefix = "report_",
    cdmName= str_to_upper(databaseName)
  )
  print("CDM connection established!")

  print("Beginning to build report sections...")
  results <- createReportSections(
    connectionDetails = connectionDetails,
    connectionDetailsMerge = connectionDetailsMerge,
    connectionDetailsOhdsi = connectionDetailsOhdsi,
    cdm,
    cdmDatabaseSchema = cdmDatabaseSchema,
    databaseName = databaseName,
    databaseId = databaseName,
    outputFolder = outputFolder,
    verboseMode = verboseMode
  )
  print("Sections complete!")

  print("Stiching sections together into complete document...")
  generateResultsDocument(
    results,
    connectionDetails = connectionDetails,
    outputFolder = outputFolder,
    authors=authors,
    databaseId = databaseId,
    databaseName = databaseName,
    databaseDescription = databaseDescription,
    smallCellCount = smallCellCount
  )
  print(paste0("Report finished for ", site))
}

