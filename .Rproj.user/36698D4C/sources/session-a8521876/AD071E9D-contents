# server.R

library(shiny)
library(DBI)
#library(odbc)
library(dplyr)
library(DT)
library(stringr)
library(htmltools)
library(RSQLite)
library(tidyr)

server <- function(input, output, session) {
  
  # Get User's ID
  annotator_username <- Sys.getenv("USERNAME")
  
  
  # # Initialize reactive values to store filtered data and annotations
  rv <- reactiveValues(
    currentInstanceIndex = 1, # To track which instance is currently being reviewed
    annotations = data.frame(
      InstanceId = character(),
      AnnotatedPrediction = character(),
      Affirmed = integer(),
      AnnotatorComment = character(),
      AllContextRace = character(),
      AllContextPrediction = character(),
      AllContextComment = character(),
      AnnotatorUsername = character(),
      stringsAsFactors = FALSE
    ),
    selectedInstance = NULL
  )
  
  
  # Reactive expression to get available terms based on other filters
  terms <- reactive({
    data <- nlp_output_table
    
    if (!is.null(input$categoryFilter) && input$categoryFilter != "") {
      data <- data %>% filter(Category1 == input$categoryFilter)
    }
    
    if (!is.null(input$predictionFilter) && input$predictionFilter != "") {
      data <- data %>% filter(Prediction == input$predictionFilter)
    }
    
    if (!is.null(input$probabilityFilter)) {
      data <- data %>% filter(Probability >= input$probabilityFilter[1], Probability <= input$probabilityFilter[2])
    }
    
    if (nrow(data) == 0) {
      showNotification("No matching data for the selected filters.", type = "warning")
      return(NULL)
    }
    
    unique(data$Normalized_Term)
  })
  
  # Reactive expression to get the available categories
  categories <- reactive({
    data <- nlp_output_table
    
    if (!is.null(input$termFilter) && input$termFilter != "") {
      data <- data %>% filter(Normalized_Term == input$termFilter)
    }
    
    if (!is.null(input$predictionFilter) && input$predictionFilter != "") {
      data <- data %>% filter(Prediction == input$predictionFilter)
    }
    
    if (!is.null(input$probabilityFilter)) {
      data <- data %>% filter(Probability >= input$probabilityFilter[1], Probability <= input$probabilityFilter[2])
    }
    
    if (nrow(data) == 0) {
      showNotification("No matching data for the selected filters.", type = "warning")
      return(NULL)
    }
    
    unique(data$Category1)
  })
  
  # Reactive expression to get the available predictions
  predictions <- reactive({
    data <- nlp_output_table
    
    if (!is.null(input$termFilter) && input$termFilter != "") {
      data <- data %>% filter(Normalized_Term == input$termFilter)
    }
    
    if (!is.null(input$categoryFilter) && input$categoryFilter != "") {
      data <- data %>% filter(Category1 == input$categoryFilter)
    }
    
    if (!is.null(input$probabilityFilter)) {
      data <- data %>% filter(Probability >= input$probabilityFilter[1], Probability <= input$probabilityFilter[2])
    }
    
    if (nrow(data) == 0) {
      showNotification("No matching data for the selected filters.", type = "warning")
      return(NULL)
    }
    
    unique(data$Prediction)
  })
  
  # Reactive expression to get the probability range based on other filters
  probabilityRange <- reactive({
    data <- nlp_output_table
    
    if (!is.null(input$termFilter) && input$termFilter != "") {
      data <- data %>% filter(Normalized_Term == input$termFilter)
    }
    
    if (!is.null(input$categoryFilter) && input$categoryFilter != "") {
      data <- data %>% filter(Category1 == input$categoryFilter)
    }
    
    if (!is.null(input$predictionFilter) && input$predictionFilter != "") {
      data <- data %>% filter(Prediction == input$predictionFilter)
    }
    
    range(data$Probability)
  })
  
  # Update filter choices and selections
  observe({
    term_choices <- terms()
    updateSelectInput(session, "termFilter", choices = term_choices, selected = input$termFilter)
    
    category_choices <- categories()
    updateSelectInput(session, "categoryFilter", choices = category_choices, selected = input$categoryFilter)
    
    prediction_choices <- predictions()
    updateSelectInput(session, "predictionFilter", choices = prediction_choices, selected = input$predictionFilter)
    
    prob_range <- probabilityRange()
    updateSliderInput(session, "probabilityFilter", min = prob_range[1], max = prob_range[2], value = prob_range)
  })
  
  # Reactive expression for filtered data
  filtered_data <- reactive({
    data <- nlp_output_table
    
    if (!is.null(input$termFilter) && input$termFilter != "") {
      data <- data %>% filter(Normalized_Term == input$termFilter)
    }
    
    if (!is.null(input$categoryFilter) && input$categoryFilter != "") {
      data <- data %>% filter(Category1 == input$categoryFilter)
    }
    
    if (!is.null(input$predictionFilter) && input$predictionFilter != "") {
      data <- data %>% filter(Prediction == input$predictionFilter)
    }
    
    if (!is.null(input$probabilityFilter)) {
      data <- data %>% filter(Probability >= input$probabilityFilter[1], Probability <= input$probabilityFilter[2])
    }
    
    if (nrow(data) == 0) {
      showNotification("No matching data found for the selected filters.", type = "warning")
      return(NULL)
    }
    
    data
  })
  
  # Render filtered data in the DataTable
  observeEvent(input$applyFilters, {
    output$dataPreview <- renderDT({
      req(filtered_data())
      datatable(filtered_data())
    })
  })
  
  ######################
  #  Load the CSV Files
  # We will load both the instance-level CSV and the patient-level CSV at the start of the app to track which instances or patients have been reviewed.
  ####################
  
  # Load instance-level and patient-level review CSVs if they exist
  instance_reviews <- reactive({
    if (file.exists(instance_csv_path)) {
      read.csv(instance_csv_path, stringsAsFactors = FALSE)
    } else {
      data.frame(InstanceId = character(), stringsAsFactors = FALSE)  # Empty dataframe if no CSV exists
    }
  })
  
  patient_reviews <- reactive({
    if (file.exists(patient_csv_path)) {
      read.csv(patient_csv_path, stringsAsFactors = FALSE)
    } else {
      data.frame(PatientICN = character(), stringsAsFactors = FALSE)  # Empty dataframe if no CSV exists
    }
  })
  
  
  
  #observe row selection in the Data Preview
  observeEvent(input$dataPreview_rows_selected, {
    selected_row <- input$dataPreview_rows_selected
    
    if (!is.null(selected_row)) {
      rv$selectedInstance <-filtered_data()[selected_row, "InstanceID"]
    }
  })
  
  
  # Proceed to Instance-Level Review
  observeEvent(input$goToInstanceReview, {
    if (is.null(input$dataPreview_rows_selected) || length(input$dataPreview_rows_selected) == 0) {
      showNotification("Please select a row before proceeding.", type = "warning")
    } else {
      updateTabsetPanel(session, "mainTabset", selected = "Instance-Level Review")
    }
  })
  
  # Render the instance-level review UI
  output$instanceReviewUI <- renderUI({
    req(input$dataPreview_rows_selected)
    current_instance <- filtered_data()[input$dataPreview_rows_selected, ]
    
    tagList(
      h3("Instance-Level Review"),
      fluidRow(
        column(6, h4("NLP Instance Details"), verbatimTextOutput("instanceDetails")),
        column(6, h4("Snippet"), uiOutput("snippetText"))
      ),
      hr(),
      h4("Annotator Input"),
      selectInput("annotatedPrediction", "Annotated Prediction:", choices = c("Patient", "Direct Ancestor", 
                                                                              "Patient native speaker", "Patient speaks", 
                                                                              "Name", "Cannot Attribute Race/Ethnicity",
                                                                              "Patient lived or lives", "Patient was born/from",
                                                                              "Family member speaks", "Other Person", "Other Location (Deployment/Travel/etc)",
                                                                              "Other")),
      radioButtons("affirmed", "Affirmed:", choices = c("Yes" = 1, "No" = 0), inline = TRUE),
      textAreaInput("annotatorComment", "Annotator Comment:", "", width = "100%"),
      actionButton("saveAnnotation", "Save Annotation"),
      actionButton("nextInstance", "Next Instance"),
      actionButton("prevInstance", "Previous Instance")
    )
  })
  
  # Display instance details
  output$instanceDetails <- renderPrint({
    req(rv$selectedInstance)
    req(filtered_data())
    
    selected_instance <-filtered_data() %>%
      filter(InstanceID == rv$selectedInstance)
    
    cat("Instance ID:", selected_instance$InstanceID, "\n")
    cat("PatientICN:", selected_instance$PatientICN, "\n")
    cat("Term:", selected_instance$Normalized_Term, "\n")
    cat("Category:", selected_instance$Category1, "\n")
    cat("Prediction:", selected_instance$Prediction, "\n")
    cat("Probability:", selected_instance$Probability, "\n")
  })
  
  # Process and display snippet with highlighted term
  output$snippetText <- renderUI({
    req(filtered_data())
    current_instance <- filtered_data()[rv$currentInstanceIndex, ]
    raw_snippet <- current_instance$Snippet
    safe_snippet <- htmlEscape(raw_snippet)
    highlighted_snippet <- safe_snippet %>%
      str_replace_all(fixed("[term]"), "<span class='highlight'>") %>%
      str_replace_all(fixed("[/term]"), "</span>")
    HTML(paste0("<pre>", highlighted_snippet, "</pre>"))
  })
  
  # Save annotations and append to CSV
  observeEvent(input$saveAnnotation, {
    req(filtered_data())
    current_instance <- filtered_data()[input$dataPreview_rows_selected, ]
    
    instance_annotation <- data.frame(
      InstanceID = current_instance$InstanceID,
      PatientICN = current_instance$PatientICN,
      AnnotatorUsername = annotator_username,  # Include username here
      AnnotatedPrediction = input$annotatedPrediction,
      Affirmed = as.integer(input$affirmed),
      AnnotatorComment = input$annotatorComment,
      stringsAsFactors = FALSE
    )
    
    #rv$annotations <- bind_rows(rv$annotations, new_annotation)
    
    # Save to SQLite
    save_instance_annotation(instance_annotation)
    
    showNotification("Annotation saved and appended to CSV.", type = "message")
  })
  
  # Move to next instance
  observeEvent(input$nextInstance, {
    rv$currentInstanceIndex <- rv$currentInstanceIndex + 1
    if (rv$currentInstanceIndex > nrow(filtered_data())) {
      showModal(modalDialog(
        title = "Review Completed",
        "You have reviewed all instances. Proceed to Patient-Level Review.",
        easyClose = TRUE,
        footer = NULL
      ))
      updateTabsetPanel(session, "mainTabset", selected = "Patient-Level Review")
    } else {
      updateSelectInput(session, "annotatedPrediction", selected = character(0))
      updateRadioButtons(session, "affirmed", selected = character(0))
      updateTextAreaInput(session, "annotatorComment", value = "")
    }
  })
  
  # Patient-Level Review UI
  output$patientReviewUI <- renderUI({
    req(rv$annotations)
    
    # Extract unique PatientICNs from filtered_data and annotations
    patient_ids <- unique(filtered_data()$PatientICN)
    
    # Identify specific PatientICN from selected instance
    req(rv$selectedInstance)
    req(filtered_data())
    
    selected_instance <-filtered_data() %>%
      filter(InstanceID == rv$selectedInstance)
    
    tagList(
      h3("Patient-Level Review"),
      selectizeInput("patientSelect", "Select Patient ID:", choices = patient_ids, selected = selected_instance$PatientICN),
      actionButton("loadPatientData", "Load Patient Data"),
      hr(),
      uiOutput("patientDataUI")
    )
  })
  
  # Load patient data
  observeEvent(input$loadPatientData, {
    req(input$patientSelect)  # Ensure a patient is selected
    patient_id <- input$patientSelect
    
    # Get patient summary
    patient_info <- patient_table %>% filter(PatientICN == patient_id)
    
    if (nrow(patient_info) == 0) {
      showNotification("No patient data found for the selected ID.", type = "warning")
      return(NULL)
    }
    
    # Render patient data UI
    output$patientDataUI <- renderUI({
      tagList(
        h4("Patient Summary"),
        verbatimTextOutput("patientSummary"),
        hr(),
        h4("NLP NHPI Data Summary"),
        tableOutput("raceDataSummary"),
        hr(),
        h4("Structured Race Data Summary"),
        tableOutput("strcRaceDataSummary"),
        hr(),
        h4("AllContext Assessment"),
        selectInput("allContextRace", "AllContextRace:", choices = c("Documented", "Suggested", "Unclear", "Negated")),
        textInput("allContextPrediction", "AllContext Prediction:"),
        textAreaInput("allContextComment", "AllContext Comment:", "", width = "100%"),
        actionButton("saveAllContext", "Save AllContext Annotation"),
        actionButton("goToNextInstanceReview", "Proceed to Next Instance-Level Review")
      )
    })
    
    # Output patient summary
    output$patientSummary <- renderPrint({
      cat("Patient ID:", patient_info$PatientICN, "\n")
      cat("Current Address:", patient_info$Location_City, ",", patient_info$Location_State, ",", patient_info$Location_Country, "\n")
      cat("Birthplace:", patient_info$BirthCity, ",", patient_info$BirthState, "\n")
      cat("Patient Name:", patient_info$PatientName, "\n")
      cat("Father Name:", patient_info$FatherName, "\n")
      cat("Mother's Maiden Name:", patient_info$MotherMaidenName, "\n")
      cat("Most Recent Facility:", patient_info$InstitutionName, ": ", patient_info$FacilityCity, ",", patient_info$FacilityState, "\n")
    })
    
    
    # Render race data summary with clickable links for the NLP_ columns
    output$raceDataSummary <- renderTable({
      nlp_race_summary <- NLP_Output_Summary %>% filter(PatientICN == input$patientSelect) %>% 
        select(-PatientICN)
      
      
      # Create clickable links for each NLP_ column
      nlp_race_summary <- nlp_race_summary %>%
        mutate(
          API = ifelse(API > 0, 
                       paste0("<a href='#' onclick=\"Shiny.setInputValue('show_snippets', {category: 'API', prediction: '", Prediction, "', count: ", API, "}, {priority: 'event'})\">", API, "</a>"), 
                       API),
          NHPI = ifelse(NHPI > 0, 
                        paste0("<a href='#' onclick=\"Shiny.setInputValue('show_snippets', {category: 'NHPI', prediction: '", Prediction, "', count: ", NHPI, "}, {priority: 'event'})\">", NHPI, "</a>"), 
                        NHPI),
          Asian = ifelse(Asian > 0, 
                         paste0("<a href='#' onclick=\"Shiny.setInputValue('show_snippets', {category: 'Asian', prediction: '", Prediction, "', count: ", Asian, "}, {priority: 'event'})\">", Asian, "</a>"), 
                         Asian),
          Polynesian = ifelse(Polynesian > 0, 
                              paste0("<a href='#' onclick=\"Shiny.setInputValue('show_snippets', {category: 'Polynesian', prediction: '", Prediction, "', count: ", Polynesian, "}, {priority: 'event'})\">", Polynesian, "</a>"), 
                              Polynesian),
          Melanesian = ifelse(Melanesian > 0, 
                              paste0("<a href='#' onclick=\"Shiny.setInputValue('show_snippets', {category: 'Melanesian', prediction: '", Prediction, "', count: ", Melanesian, "}, {priority: 'event'})\">", Melanesian, "</a>"), 
                              Melanesian),
          Micronesian = ifelse(Micronesian > 0, 
                               paste0("<a href='#' onclick=\"Shiny.setInputValue('show_snippets', {category: 'Micronesian', prediction: '", Prediction, "', count: ", Micronesian, "}, {priority: 'event'})\">", Micronesian, "</a>"), 
                               Micronesian)
        )
      
      nlp_race_summary
    }, sanitize.text.function = function(x) x)  # Render HTML content
    
    
    # Reactive values to track pagination
    snippet_pagination <- reactiveValues(
      page = 1,        # Current page
      total_pages = 1  # Total number of pages
    )
    
    # Limit the Snippets per page
    # Show the modal with paginated snippets when a category and prediction are clicked
    observeEvent(input$show_snippets, {
      req(input$show_snippets)
      
      # Print the clicked category and prediction for debuggin
      print(paste("Category:", input$show_snippets$category))
      print(paste("Prediction:", input$show_snippets$prediction))
      
      
      # Get the clicked category and prediction
      selected_category <- input$show_snippets$category
      selected_prediction <- input$show_snippets$prediction
      patient_id <- input$patientSelect
      
      # Filter the relevant snippets from nlp_output_table based on the category, prediction, and patient
      relevant_snippets <- nlp_output_table_All %>%
        filter(PatientICN == patient_id, Category1 == selected_category, Prediction == selected_prediction) %>%
        select(Snippet)
      
      total_snippets <- nrow(relevant_snippets)
      
      print(paste("Number of relevant snippets:", total_snippets))
      
      # Calculate total pages needed (10 snippets per page)
      snippet_pagination$total_pages <- ceiling(total_snippets / 10)
      snippet_pagination$page <- 1  # Reset to the first page
      
      # Function to display snippets for the current page
      show_snippets_for_page <- function(page) {
        start_index <- (page - 1) * 10 + 1
        end_index <- min(page * 10, total_snippets)
        
        # Extract the relevant snippets for the current page
        snippets_to_display <- relevant_snippets$Snippet[start_index:end_index]
        
        # Generate HTML content for snippets
        HTML(paste(snippets_to_display, collapse = "<hr>"))
      }
      
      # Generate the modal with paginated snippets
      showModal(modalDialog(
        title = paste("Snippets for", selected_category, "and", selected_prediction),
        renderUI({
          tagList(
            h4(paste("Total Snippets:", total_snippets)),
            uiOutput("snippetContent"),  # Placeholder for the snippet content
            hr(),
            # Pagination controls
            actionButton("prevPage", "Previous", style = "float:left;"),
            actionButton("nextPage", "Next", style = "float:right;"),
            div(style = "text-align:center;", paste("Page", snippet_pagination$page, "of", snippet_pagination$total_pages))
          )
        }),
        easyClose = TRUE,
        footer = modalButton("Close")
      ))
      
      # Render the current page of snippets
      output$snippetContent <- renderUI({
        show_snippets_for_page(snippet_pagination$page)
      })
      
      # Previous Page Button
      observeEvent(input$prevPage, {
        if (snippet_pagination$page > 1) {
          snippet_pagination$page <- snippet_pagination$page - 1
          output$snippetContent <- renderUI({
            show_snippets_for_page(snippet_pagination$page)
          })
        }
      })
      
      # Next Page Button
      observeEvent(input$nextPage, {
        if (snippet_pagination$page < snippet_pagination$total_pages) {
          snippet_pagination$page <- snippet_pagination$page + 1
          output$snippetContent <- renderUI({
            show_snippets_for_page(snippet_pagination$page)
          })
        }
      })
    })
    
    
    # Output race data summary
    strc_race_summary <- race_data_summary %>% filter(PatientICN == patient_id) %>% 
      select(c("STRC_API", "STRC_Micronesian", "STRC_Polynesian", "STRC_NHPI", "STRC_NonNHPIRace"))
    output$strcRaceDataSummary <- renderTable({
      strc_race_summary
    })
  })
  
  # Save AllContext annotation
  observeEvent(input$saveAllContext, {
    patient_id <- input$patientSelect
    
    # Find instance_id that led to the review of this patient
    req(filtered_data())
    current_instance <- filtered_data()[rv$currentInstanceIndex, ]
    relevant_instance <- current_instance$InstanceID
    
    # Create the patient-level annotation data-frame
    patient_annotation <- data.frame(
      PatientICN = patient_id,
      InstanceID = relevant_instance,
      AllContextRace =input$allContextRace,
      AllContextPrediction = input$allContextPrediction,
      AllContextComment = input$allContextComment,
      stringsAsFactors = FALSE
    )
    
    
    # Save to SQLite
    save_patient_annotation(patient_annotation)
    
    showNotification("Patient-level annotation saved.", type = "message")
  })
  
  # # Generate final CSV output
  # observeEvent(input$generateCSV, {
  #   # Write annotations to CSV
  #   manage_csv(rv$annotations)
  #   showNotification("CSV output generated: annotations_output.csv", type = "message")
  # })
  
  
  ########
  # Visual Indicator for Review Status
  # Use a visual indicator (colored box) to show the review status (yellow for unreviewed, green for reviewed).
  ########
  
  # Render the review status box in instance-level review
  output$instanceReviewStatus <- renderUI({
    req(rv$selectedInstance)
    
    # Check if the instance has been reviewed
    if (rv$selectedInstance %in% instance_reviews()$InstanceId) {
      div(style = "background-color: green; color: white; padding: 10px;", "Reviewed")
    } else {
      div(style = "background-color: yellow; padding: 10px;", "Unreviewed")
    }
  })
  
  # Render the review status box in patient-level review
  output$patientReviewStatus <- renderUI({
    req(input$patientSelect)
    
    # Check if the patient has been reviewed
    if (input$patientSelect %in% patient_reviews()$PatientICN) {
      div(style = "background-color: green; color: white; padding: 10px;", "Reviewed")
    } else {
      div(style = "background-color: yellow; padding: 10px;", "Unreviewed")
    }
  })
  
  #Navigate to next instance-level review from Patient-level review
  observeEvent(input$goToNextInstanceReview, {
    if (rv$currentInstanceIndex < nrow(filtered_data())) {
      rv$currentInstanceIndex <-rv$currentInstanceIndex +1
    } else {
      showNotification("This is the last instance.", type = "warning")
    }
    
    # Re-render instance details and snippet
    output$instanceDetails <- renderPrint({
      currentInstance <-filtered_data()[rv$currentInstanceIndex, ]
      
      cat("Instance ID:", currentInstance$InstanceID, "\n")
      cat("PatientICN:", currentInstance$PatientICN, "\n")
      cat("Term:", currentInstance$Normalized_Term, "\n")
      cat("Category:", currentInstance$Category1, "\n")
      cat("Prediction:", currentInstance$Prediction, "\n")
      cat("Probability:", currentInstance$Probability, "\n")
    })
    
    output$snippetText <- renderUI({
      currentInstance <- filtered_data()[rv$currentInstanceIndex, ]
      raw_snippet <- currentInstance$Snippet
      safe_snippet <- htmlEscape(raw_snippet)
      highlighted_snippet <- safe_snippet %>%
        str_replace_all(fixed("[term]"))
    })
    output$snippetText <- renderUI({
      req(filtered_data())
      current_instance <- filtered_data()[rv$currentInstanceIndex, ]
      raw_snippet <- current_instance$Snippet
      safe_snippet <- htmlEscape(raw_snippet)
      highlighted_snippet <- safe_snippet %>%
        str_replace_all(fixed("[term]"), "<span class='highlight'>") %>%
        str_replace_all(fixed("[/term]"), "</span>")
      HTML(paste0("<pre>", highlighted_snippet, "</pre>"))
    })
  })
  
  # Move to previous instance
  observeEvent(input$prevInstance, {
    if (rv$currentInstanceIndex > 1) {
      rv$currentInstanceIndex <- rv$currentInstanceIndex - 1
    } else {
      showNotification("You are already at the first instance.", type = "warning")
    }
    # Re-render instance details and snippet
    output$instanceDetails <- renderPrint({
      currentInstance <-filtered_data()[rv$currentInstanceIndex, ]
      
      cat("Instance ID:", currentInstance$InstanceID, "\n")
      cat("PatientICN:", currentInstance$PatientICN, "\n")
      cat("Term:", currentInstance$Normalized_Term, "\n")
      cat("Category:", currentInstance$Category1, "\n")
      cat("Prediction:", currentInstance$Prediction, "\n")
      cat("Probability:", currentInstance$Probability, "\n")
    })
    
    output$snippetText <- renderUI({
      currentInstance <- filtered_data()[rv$currentInstanceIndex, ]
      raw_snippet <- currentInstance$Snippet
      safe_snippet <- htmlEscape(raw_snippet)
      highlighted_snippet <- safe_snippet %>%
        str_replace_all(fixed("[term]"))
    })
    output$snippetText <- renderUI({
      req(filtered_data())
      current_instance <- filtered_data()[rv$currentInstanceIndex, ]
      raw_snippet <- current_instance$Snippet
      safe_snippet <- htmlEscape(raw_snippet)
      highlighted_snippet <- safe_snippet %>%
        str_replace_all(fixed("[term]"), "<span class='highlight'>") %>%
        str_replace_all(fixed("[/term]"), "</span>")
      HTML(paste0("<pre>", highlighted_snippet, "</pre>"))
    })
  })

  # Close database connection when app stops
  session$onSessionEnded(function() {
    dbDisconnect(con)
  })
}