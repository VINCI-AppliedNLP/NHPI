############## Create BackEnd Data #####################
# 
#  Use PostGreSQL DB for backend (should be relatively easy to change to SQL Server)
#
#
#
#########################################################


######## Postgres ##################
# Install and load required libraries
# install.packages("readxl")
# install.packages("RPostgres")
# install.packages("DBI")

library(readxl)
library(RPostgres)
library(DBI)

# Set up PostgreSQL connection details
password = ""

# Connect to default PostgreSQL database to create a new database
con_default <- dbConnect(RPostgres::Postgres(),
                         host = "localhost",
                         port = "5432",
                         dbname = "postgres",
                         user = "u0807390",
                         password = password)

# Create new database
dbExecute(con_default, "CREATE DATABASE nhpi_tool;")


# Connect to PostgreSQL
con <- dbConnect(RPostgres::Postgres(),
                 host = "localhost",
                 port = "5432",
                 dbname = "nhpi_tool",
                 user = "u0807390",
                 password = password)

#Create Schema for sample backend data
dbExecute(con, "CREATE SCHEMA backend;")

# File path for your Excel file
file_path <- "./BackEndExcel.xlsx"

# Get all sheet names
sheet_names <- excel_sheets(file_path)

# Loop through each sheet and create a table in PostgreSQL
for (sheet in sheet_names) {
  # Read each sheet into a data frame
  data <- read_excel(file_path, sheet = sheet)
  
  # Write the data frame to PostgreSQL
  dbWriteTable(con, Id(schema = "backend", table = sheet), value = data, overwrite = TRUE, row.names = FALSE)
}


#Create schema for annotations
dbExecute(con, "CREATE SCHEMA annotate;")

# Disconnect from PostgreSQL
dbDisconnect(con)
