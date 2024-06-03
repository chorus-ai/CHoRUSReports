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
#' @param container_url                       Web location of Azure storage account
#' @param sas                                 SAS key for the Azure storage account
#' @param container_name                      Name of the blob container in question
#' @return                                    An object containing the files and their metadata in the relevant storage container on Azure
#' @export
#'

list_blobs <- function(container_url, sas, container_name) {
  # Using AzureStor package
  library(AzureStor)
  
  blob_endpoint <- paste0("https://", container_url, "/")
  fl_endp_sas <- AzureStor::storage_endpoint(blob_endpoint, sas=sas)
  blob_container <- AzureStor::storage_container(fl_endp_sas, container_name)
  
  # List the blob names
  #blob_list <- list_blobs(blob_container)
  files <- AzureStor::list_storage_files(blob_container, recursive=TRUE, info="all")
  
  return(files)
}

