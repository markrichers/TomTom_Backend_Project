# 🌍 TomTom Data Warehouse Backend Project

<p align="center">
  <img src="https://github.com/user-attachments/assets/c8e3f127-22b6-4b43-9e98-c9c0861682bf" alt="TomTom Logo" width="200"/>
</p>

---

<p align="center">
  <b>Powering Location Intelligence with Scalable Backend Solutions</b>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Framework-FastAPI-blue?style=flat-square" alt="FastAPI">
  <img src="https://img.shields.io/badge/Database-PostgreSQL-green?style=flat-square" alt="PostgreSQL">
  <img src="https://img.shields.io/badge/Cloud-AWS-orange?style=flat-square" alt="AWS">
  <img src="https://img.shields.io/badge/License-MIT-blue?style=flat-square" alt="License">
</p>

---

## 🌟 About the Project

The **TomTom Backend Project** is a robust backend solution designed for processing and managing large-scale geospatial data. With a focus on scalability, reliability, and performance, this project integrates advanced technologies to deliver location intelligence services efficiently.

![Screenshot from 2023-08-25 12-52-22](https://github.com/markrichers/TomTom_Backend_Project/assets/50198601/8cd2d84d-c62a-4333-8cc7-badf4eb9cbe4)


🔄 Data Flow of the Project
1. 📤 Supplier Upload Data
Suppliers upload raw data, including country/city information, to the interface shown in Figure 15, Chapter 5. The system verifies the data before it is added to the GRIP application's data source, preparing it for further processing.

2. 🔄 Data Mapping
The system converts the raw driver data into a consistent schema and model, as detailed in Chapter 4.3. This makes it easier to compare and analyze geographic information from various traffic suppliers’ resources.

3. 📥 Extract Data
In the extraction phase, rules are extracted from the YARD Database, which is already integrated into the system. The system then compares this data with raw driver data from suppliers/customers, checking various mapping rules such as street names, addresses, and location rule comparisons.

4. 🧪 Quality Checks
The combined dataset undergoes Quality Checks, where potential violations of traffic rules or discrepancies in the data are analyzed. The system compares traffic regulation rules with the driver data to identify issues.

5. ✅ Evaluation Check
The processed data is cross-referenced with each country’s traffic rules to ensure drivers comply with regulations. Testing determines whether the data quality is up to standard.

6. 📊 Send Data Quality Report
If drivers are found to be violating traffic rules (e.g., exceeding speed limits or violating size and weight restrictions), a report is generated and sent to the supplier, highlighting these instances.

7. 📤 Delivery Data
The final cleaned data is delivered to the Map Data Product Data Location, also known as the Quality Table. This data is then restructured and prepared for customer/supplier use.

8. 💻 Send Clean Data to Customers
The cleaned and processed data is sent to customers for their use and further analysis.

9. 💾 Saving Backup Data Results
The findings and suggested improvements based on the final cleaned data are shared and saved to the GRIP Database. This step ensures that the data output is preserved for future updates, daily changes, or monthly traffic rule processing.

## Data Table: 

# Appendix 1: Table Detail on Columns Traffic

| **Field Name**            | **Description**                                                                 |
|----------------------------|---------------------------------------------------------------------------------|
| ID                        | Unique identifier for each record.                                              |
| Check_id                  | Identifier for the check that was performed.                                    |
| Status                    | Status of the check (Passed, Failed, etc.).                                     |
| Fail_meter                | Meter reading when the check failed (if it failed).                             |
| Start_time                | The time when the check started.                                                |
| End_time                  | The time when the check ended.                                                  |
| Processing_time           | The total time it took to perform the check.                                    |
| Job_ID                    | Identifier for the job associated with the check.                               |
| Internal_id               | Internal identifier used for system purposes.                                   |
| Comment                   | Any notes or comments related to the check.                                     |
| Max_dim                   | Maximum dimension checked.                                                      |
| Internal_id               | Another internal identifier used for system purposes.                           |
| Iso_countrycode           | The ISO country code related to the check.                                      |
| Maxdim1_type              | Type of the first maximum dimension checked.                                    |
| Maxdim1_value             | Value of the first maximum dimension checked.                                   |
| Maxdim2_type              | Type of the second maximum dimension checked.                                   |
| Maxdim2_value             | Value of the second maximum dimension checked.                                  |
| Country_Code              | Country code where the check was performed.                                     |
| Dimension_type            | Type of the dimension being checked (Height, Length, Width, etc.).              |
| Lower_limit               | Lower limit of the acceptable range for the dimension.                          |
| Upper_limit               | Upper limit of the acceptable range for the dimension.                          |
| Lower_limit_motorways     | Lower limit of the acceptable range for the dimension on motorways.             |
| Allowed_on_motorways      | Indicator if the dimension is allowed on motorways (Yes, No).                   |

## Code to extract data: 

![image](https://github.com/user-attachments/assets/e780343d-aa44-48cd-bf08-6ae5b87dbec8)


### Data Modelling: 

![Data Modelling](https://github.com/user-attachments/assets/2e1d2a15-c239-40f1-8b5c-6fa418e314b7)

### Database Frame: 

![GRIP Database](https://github.com/user-attachments/assets/31d1b0bf-b6cc-4e56-a656-12c984f94a64)

### Testing Data Quality: 

![Picture1](https://github.com/user-attachments/assets/5041ef35-b3f4-4240-ba9b-2eef5cef0883)

### Result Outcome:

![DataDashboard](https://github.com/user-attachments/assets/e5b4dd27-8fbd-407d-93fb-6533d578bddd)

### Result Outcome 2: 

![Dashboard 2](https://github.com/user-attachments/assets/7c19f673-af87-45f1-8ba7-3ad0e103448a)






