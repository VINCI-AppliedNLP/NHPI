# ui.R

library(shiny)
library(DT)

ui <- fluidPage(
  titlePanel("NHPI Race/Ethnicity Classification Tool"),
  
  # Include custom CSS for highlighting
  tags$head(
    tags$style(HTML("
      .highlight {
        background-color: yellow;
        font-weight: bold;
      }
      pre {
        white-space: pre-wrap;
        word-wrap: break-word;
      }
      #snippetText pre {
        max-height: 300px;
        overflow-y: auto;
      }
    "))
  ),
  
  sidebarLayout(
    sidebarPanel(
      h4(paste("Annotator:", Sys.getenv("USERNAME"))
      ),
      h3("Data Filtering"),
      selectInput("termFilter", "Term:", choices = NULL, selected = NULL, multiple = TRUE),
      selectInput("categoryFilter", "Category:", choices = NULL, selected = NULL, multiple = TRUE),
      selectInput("predictionFilter", "Prediction:", choices = NULL, selected = NULL, multiple = TRUE),
      sliderInput("probabilityFilter", "Probability Range:", min = 0, max = 1, value = c(0, 1)),
      actionButton("applyFilters", "Apply Filters"),
      hr(),
      actionButton("goToInstanceReview", "Proceed to Instance-Level Review")
    ),
    
    mainPanel(
      tabsetPanel(
        id = "mainTabset",
        tabPanel("Data Preview",
                 DTOutput("dataPreview")
        ),
        tabPanel("Instance-Level Review",
                 uiOutput("instanceReviewUI")
        ),
        tabPanel("Patient-Level Review",
                 uiOutput("patientReviewUI")
        )
      )
    )
  )
)