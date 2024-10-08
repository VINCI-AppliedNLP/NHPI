# global.R

library(shiny)
#library(odbc)
library(dplyr)
library(DT)
library(stringr)
library(htmltools)
library(tidyr)

library(RPostgres)
library(DBI)

# Set up PostgreSQL connection details
host <- "localhost"
port <- "5432"
dbname <- "nhpi_tool"
user <- "u0807390"

# Connect to PostgreSQL
create_connection <- function() {
  dbConnect(RPostgres::Postgres(),
            host = host,
            port = port,
            dbname = dbname,
            user = user,
            password = "life5433")
}

###########  Load data from SQLite upon app startup ##############
#RSQLite::dbListTables(con)


con <- create_connection()
PatientSummary <- dbReadTable(con, Id(schema = "backend", table ="nlp_output_table_ALL")) %>%
  group_by(PatientICN, Category1, Prediction) %>%
  summarize(Ct = n())

nlp_output_table <- dbReadTable(con, Id(schema = "backend", table ="nlp_output_table_ALL"))

nlp_output_table_All <- nlp_output_table

race_data_summary <- dbReadTable(con, Id(schema = "backend", table ="race_data_summary"))

NLP_Output_Summary <-PatientSummary %>%
  pivot_wider(
    names_from = Category1,
    values_from = Ct,
    values_fill = 0
  )

patient_table <- dbReadTable(con, Id(schema = "backend", table ="patient_table"))


# Function to manage CSV creation and appending
#username <- Sys.getenv("USERNAME")
username <- "User #1"



# Create tables if they don't already exist
dbExecute(con, "
  CREATE TABLE IF NOT EXISTS annotate.instance_annotations (
    InstanceId TEXT PRIMARY KEY,
    AnnotatorUsername TEXT,
    AnnotatedPrediction TEXT,
    Affirmed INTEGER,
    AnnotatorComment TEXT
  )
")

dbExecute(con, "
  CREATE TABLE IF NOT EXISTS annotate.patient_annotations (
    PatientICN TEXT,
    InstanceId TEXT,
    AllContextRace TEXT,
    AllContextPrediction TEXT,
    AllContextComment TEXT
  )
")

# Disconnect from the database
dbDisconnect(con)

# query <- sprintf("INSERT INTO annotate.patient_annotations (PatientICN, InstanceId, AllContextRace, AllContextPrediction, AllContextComment) VALUES ('%s', '%s', '%s', '%s', '%s')",
#                  '123',
#                  '123-1',
#                  'Unclear',
#                  'Non-NHPI',
#                  'No data to indicate except Hawaii birthplace')
# dbExecute(con, query)

con <- create_connection()
query <- sprintf("INSERT INTO annotate.patient_annotations (PatientICN, InstanceId, AllContextRace, AllContextPrediction, AllContextComment) VALUES ('%s', '%s', '%s', '%s', '%s')",
                 1000006,
                 "1001",
                 "Documented",
                 "Chamorro - Micronesian",
                 "All data points towards Micronesian")
dbExecute(con, query)

# Function to save instance-level annotations using an INSERT query
save_instance_annotation <- function(annotation) {
  con <- create_connection()
  tryCatch({
    query <- sprintf("INSERT INTO annotate.instance_annotations (InstanceId, AnnotatorUsername, AnnotatedPrediction, Affirmed, AnnotatorComment) VALUES ('%s', '%s', '%s', %d, '%s')",
                       annotation$InstanceID,
                       annotation$AnnotatorUsername,
                       annotation$AnnotatedPrediction,
                       annotation$Affirmed,
                       annotation$AnnotatorComment)
    dbExecute(con, query)
    print('Write successful')
  }, error = function(e) {
    stop(paste('Error writing instance annotation:', e$message))
  }, finally = {
    dbDisconnect(con)
  })
}

# Function to save patient-level annotations using an INSERT query
save_patient_annotation <- function(annotation) {
  con <- create_connection()
  tryCatch({
    # Debug by printing contents of annotation list
    print("Attempting to save patient annotation with the following data:")
    print(annotation)
    
    query <- sprintf("INSERT INTO annotate.patient_annotations (PatientICN, InstanceId, AllContextRace, AllContextPrediction, AllContextComment) VALUES ('%s', '%s', '%s', '%s', '%s')",
                       annotation$PatientICN,
                       annotation$InstanceID,
                       annotation$AllContextRace,
                       annotation$AllContextPrediction,
                       annotation$AllContextComment)
    #Debug query construction:
    print("Generated SQL Query:")
    print(query)
    
    dbExecute(con, query)
    print('Write successful')
  }, error = function(e) {
    stop(paste('Error writing patient annotation:', e$message))
  }, finally = {
    dbDisconnect(con)
  })
}

# Close database connection when done
#on.exit(dbDisconnect(con), add = TRUE)
