<b>Introduction</b>

The motivation for this project is to develop an ETL process using dbt and SSMS to create a DWH.
I used Wide World Importers db from microsoft.
S2T mapping requirements is in a the documentation folder


<b>Project Description </b>

In this project i used three stages to build the DWH:

    * 0_mrr - This stage is the exact copy of the tables from the source database used for the ETL project

    * 1_stg - This stage is the transformation stage where i used the source tables to create the tables in the warehouse

    * 2_dwh - This is the final stage where i have implemented incrmental load for each table using its key

