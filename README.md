# Formula One Race Data Engineering Project

## Overview
This project showcases the application of the Data Engineering Lifecycle in processing and analyzing Formula 1 racing data. It involves managing data for both driver champions and constructor champions. Points are awarded based on finishing positions in each race, with separate championships for drivers and constructors. Pole positions are determined through qualifying races.

----

## Data Source
The data is sourced from the **Ergast Developer API**, which provides tables such as:
- Circuits
- Races
- Constructors
- Drivers
- Results
- Pitstops
- Lap Times
- Qualifying

--- 

## ER Diagram
![image](https://github.com/user-attachments/assets/604326b3-c9be-49a6-bf3b-fb23e6885a3e)

--- 
## Project Resources and Architecture

### Azure Services Used
1. **Azure Databricks**: For compute using Python and SQL.
2. **Azure Data Lake Gen2 (Delta Lake / LakeHouse)**: For hierarchical storage and utilizing delta tables.
3. **Azure Data Factory**: For pipeline orchestration for Databricks Notebooks.
4. **Azure Key Vault**: For securely storing ADLS credentials used in the Databricks Notebooks.

---

### Project Architecture
![image](https://github.com/user-attachments/assets/d9941927-83bf-4cfa-bd1b-7f7d0fd7d271)

---

### Architecture Explanation & Project Workflow

#### **Step 1: Setting up Storage Using ADLS and Containers**
- Created **Azure Data Lake Storage** with three containers:
  - **Raw**: For raw data ingestion.
  - **Processed**: For cleaned and transformed data.
  - **Presentation**: For final data ready for analysis.

#### **Step 2: Setting up Compute Using Databricks and Connecting to Storage**
- Utilized **Azure Databricks** with a specific cluster configuration.
- Mounted Azure Data Lake using **Service Principal** for secure access.

#### **Step 3: Ingesting Raw Data**
- Ingested eight types of files and folders from the raw container.
- Created separate notebooks for ingestion.
- Converted raw data into processed data.

#### **Step 4: Ingesting Processed Data**
- Used processed data for further transformation and analysis.
- Created notebooks for:
  - Race results
  - Driver standings
  - Constructor standings
  - Calculated race results

#### **Step 5: Ingesting Presentation Data for Analysis**
- Stored data generated from processed notebooks in the presentation container.
- Analyzed and visualized dominant drivers and teams.

---

### Azure Data Factory Pipeline Orchestration

#### **Pipelines and Triggers**
1. **Ingest Pipeline**: Automates raw data ingestion.
2. **Transform Pipeline**: Manages data transformation.
3. **Process Pipeline**: Ensures the final data is prepared for analysis.

#### **Azure Trigger**
- Used **Tumbling Window Trigger** to automate processing based on historical data.
- Customized triggers for specific date ranges.

---

## Goals acheived
- Develop expertise in Azure Databricks, Delta Lake, and Azure Data Factory.
- Automate data processing and pipeline orchestration.
- Create insightful dashboards and visualizations for Formula 1 race data.

---

## Technologies and Tools
- **Microsoft Azure Portal**
- **Ergast Developer API**
- **Azure Databricks**
- **Azure Data Lake Gen2**
- **Azure Data Factory**
- **Azure Key Vault**
- **Power BI**

---

## Future Enhancements
- Integration with live API data for real-time insights.
- Predictive modeling for race outcomes.
- Advanced visualizations showcasing team and driver performance trends
