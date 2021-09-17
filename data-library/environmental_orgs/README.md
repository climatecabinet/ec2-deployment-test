# Environmental Orgs Data Pipeline
This pipeline fetches and cleans a list of environmental group names, prepping them to be joined and loaded onto state documents in the database.

### 1. What is the source of this dataset? (Include a link if possible)
The source of this dataset is a Google Sheet built manually by Niles Egan \(niles@climatecabinet.org\), which can be [found here](t.ly/uiMQ).

### 2. How should this data be joined/ loaded into the database?
This data is indexed on states (specifically state abbreviations), with 1-2 groups per state. When loading this data into the database, the build script should retrieve the State document for the state in each row and update that document's **environmental_organizations** embedded document list.

### 3 . How should the fetched/ cleaned data be stored in the **data** folder?
Storing the database-ready data as a single csv in the **data** folder should be fine.

### 4. How to fetch this data from the world?
Running the command `python run.py fetch environmental_orgs` will fetch the data.

### 5. How to clean this data?
Running the command `python run.py clean environmental_orgs` will clean the data.
