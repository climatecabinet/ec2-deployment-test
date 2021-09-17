# ec2-deployment-test
A copy of our main ETL pipeline and database building repository, but reduced in scope to only a handful of pipelines.

## Setup
Start by making a new virtual environment and activating it
```sh
virtualenv .env
source .env/bin/activate
```

Then, install dependencies
```sh
pip install -r requirements
```

## Usage
To make a new database from scratch, use the command
```sh
python run.py new-db -l
```
to specify a name for this new database, add the `-db` flag
```sh
python run.py new-db -l -db my-new-database
```