# TomTom-Backend-Project
![Screenshot from 2023-08-25 12-52-22](https://github.com/markrichers/TomTom_Backend_Project/assets/50198601/8cd2d84d-c62a-4333-8cc7-badf4eb9cbe4)


1. Supplier Upload Data
Suppliers initially upload raw data from country/city information to the interface in Figure 15, chapter 5,
which requires verification via the system. This raw data is added to the source of the GRIP application,
preparing it for processing.
2. Data Mapping
The system converts the raw driver data into a consistent data schema format and modeling, which is detail
on chapter 4.3, making it easier to compare and analyze geographic information from various traffic
supplier’ resource.
3. Extract Data
The extraction phase extracts rules from the YARD Database already in the system. It then compares this
with the raw driver data from Suppliers/Customers, looking at various mapping rules such as street name,
address, and location rule comparisons.
4. Quality Checks
The combined dataset is analyzed to identify potential violations of traffic rules or any discrepancies in the
data. This part, called Quality Checks, compares traffic regulation rules with driver data.
5. Evaluation Check
The processed data is cross-referenced with each country's traffic rules to check whether drivers adhere to
the regulations. Testing determines whether the data quality is of a good standard.
6. Send a report of data qualitypg. 23
Any instances where drivers are not following traffic rules, such as exceeding speed limits or violating size
and weight restrictions, are highlighted documents to provide the supplier with a as report.
7. Delivery Data
The final cleaned data is delivered to the Map Data Product Data Location, referred to as the Quality Table,
then prepared and restructured for customer/supplier use.
8. Send Clean Data to Customers
The cleaned and processed data is then sent to customers for their utilization.
9. Saving Backup Data Results
The findings and suggested improvements from the final cleaned data after processing are shared and
saved to the GRIP Database. This step preserves the data output for any changes in daily updates or
monthly processing related to traffic rules.

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



### Data Modelling: 

![Data Modelling](https://github.com/user-attachments/assets/2e1d2a15-c239-40f1-8b5c-6fa418e314b7)

### Database Frame: 

![GRIP Database](https://github.com/user-attachments/assets/31d1b0bf-b6cc-4e56-a656-12c984f94a64)

### Testing Data Quality: 

![Picture1](https://github.com/user-attachments/assets/5041ef35-b3f4-4240-ba9b-2eef5cef0883)

### Result Outcome:

![DataDashboard](https://github.com/user-attachments/assets/e5b4dd27-8fbd-407d-93fb-6533d578bddd)

### Resutl Outcome 2: 

![Dashboard 2](https://github.com/user-attachments/assets/7c19f673-af87-45f1-8ba7-3ad0e103448a)






