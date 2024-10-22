Here is a draft README for your project:

---

# NHPI

NHPI (Natural Health Processing Initiative) is a project focused on applying Natural Language Processing (NLP) techniques to classify and analyze patient health records. The project includes an R Shiny tool designed to review complete patient charts. This work was originally presented at the OHDSI Global Symposeum here: https://www.ohdsi.org/2024showcase-34 


## Overview

The NHPI project aims to leverage advanced NLP methods to classify clinical text data, providing insights and improving healthcare outcomes. The project is implemented in R and includes a user-friendly interface via an R Shiny application.

## Features

- **NLP Classification**: This folder contains the R Shiny code used for the complete patient chart review of NLP output and other structured ata sources. An interactive tool built with R Shiny for reviewing complete patient charts, allowing for detailed analysis and visualization of clinical data.
- **NLP**: This folder contains some high-level notebooks and scripts used for the development of the NLP system.  Unfortunately the Model and data can not leave the VA firewall, but output of this system and the model will be available for other VA researchers. 

## Getting Started

### Prerequisites

- R and RStudio
- Required R packages: `shiny`, `tidyverse`, `caret`, `text` (and other relevant NLP packages)

### Installation

1. Clone the repository:
   ```sh
   git clone https://github.com/VINCI-AppliedNLP/NHPI.git
   ```
2. Navigate to the project directory:
   ```sh
   cd NHPI
   ```
3. Install the necessary R packages:
   ```r
   install.packages(c("shiny", "tidyverse", "caret", "text"))
   ```

### Usage

1. Start the R Shiny application:
   ```r
   shiny::runApp('path_to_shiny_app')
   ```
2. Upload patient health records and start the NLP classification process.

## Contributing

Contributions are welcome! Please submit a pull request or open an issue to discuss any changes.

## License

This project is licensed under the MIT License.

---

Feel free to modify this draft as needed to better fit your project's specifics.
