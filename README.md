# multinational-retail-data-centralisation 
My task was to produce a system that stores a given company data in a database so that it can be accessed from one centralised location and acts as the single source for sales data.

The database can be queried to get up-to-date metrics for the business.

Currently, the given sales data is spread across many different data sources making it inaccessible to its users to analyse. This is why there is a need for the sales data to be accessible from one centralised location.

## Installation
Download repo and extract zip to preferred location. 
Python is required for the functionality.
Install tabula-py, numpy, sqlalchemy, requests, JSON, and boto3.
Install pgadmin to easily view and setup local databases.

## Usage 
Open the folder of the extracted zip file and open a terminal from that folder. Read comments at the bottom of the data_extraction.py to see how to use  the functionality. Uncomment the desired comments then run the command 'python data_extraction.py' to run the program. 

Before some of the commands given in the python file can successfully execute, you will need to create a YAML file with the credentials needed by sqlalchemy to be able to create an engine used to upload and download from local and online databases. An example YAML is provided but details should be changed before use.

## File structure
```
proj2 
 ┣ database_utils.py
 ┣ data_cleaning.py
 ┣ data_extraction.py
 ┣ date_details.json
 ┣ db_creds.yaml
 ┣ debug.log 
 ┣ ldb.yaml
 ┣ milestone3.sql
 ┣ milestone4.sql 
 ┗ README.md
```

## License
N/A