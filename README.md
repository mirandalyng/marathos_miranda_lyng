
![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache_Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Plotly](https://img.shields.io/badge/Plotly-3F4F75?style=for-the-badge&logo=plotly&logoColor=white)
![Git](https://img.shields.io/badge/Git-F05032?style=for-the-badge&logo=git&logoColor=white)
![GitHub](https://img.shields.io/badge/GitHub-181717?style=for-the-badge&logo=github&logoColor=white)

# Marathos Lab Big Data Cloud Course - Medallian Architecture 

Purpose of this project is to use your knowledge of Databricks and data engineering to build data
platform and pipeline for business stakeholders to aid in data driven decisions. You are working for a
global company called Marathos, which hosts marathons all over the world.

#### Bronze 
- Ingesting data into the bronze layer 

#### Silver 

**Cleaning of the dataset:**

*Read more in the data_desisions file*

- **Event names** are stripped of leading `#`, `"`, and `*` characters; names that are empty after cleaning are replaced with `unknown`
- **Athlete club** values are stripped of leading `#`, `"`, and `*` characters; empty values are replaced with `unknown`
- **Athlete country** empty values are replaced with `unknown`
- **Athlete gender** and **age category** empty values are replaced with `unknown`
- **Event country** is extracted from the event name using the three-letter country code in parentheses, e.g. `Berlin Marathon (GER)` → `GER`, and resolved to a full country name via a country reference table
- **Athlete country** is resolved to a full country name via the same country reference table, matching on both ISO Alpha-3 code and long name
- **Data Validation:** Filtered out invalid performances (e.g., DNF / broken records) and structured the output into an optimized One Big Table (OBT).

#### Gold

- Gold layer are built based on the dimensional model 
    - star schema fct_results (streaming table) 
    - dim_tables for athlete, race, event (materalized views)
- Views are created for the marathon types (distance, length)
- Validation and debugging is made in the gold EDA for verifying the tables that are created 



#### Pipeline graph - bronze -> silver -> gold 

![Pipeline - bronze - silver - gold](pipeline.png)

### Dimensional Modeling 

The demensional modeling is done in [DB Diagram](https://dbdiagram.io/home)

![Dimensional modeling](dimensional_modeling/dimensional_modeling.png)


### Dashboard 


## Sources and Documentation 

**Raw dataset:**

[Ultra Marathon Running Dataset - Kaggle](https://www.kaggle.com/datasets/aiaiaidavid/the-big-dataset-of-ultra-marathon-running)

[Country Raw dataset iso 3166 - GitHub](https://github.com/ipregistry/iso3166.git)

**Sources:**

[Databricks documentation](https://docs.databricks.com/aws/en/)

[Apache Spark doucmentation](https://spark.apache.org/docs/latest/api/python/index.html)

LMM is used for Debugging, regexp and creating summarys for readme/data_decisions file. 



