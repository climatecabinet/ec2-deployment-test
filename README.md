# ec2-deployment-test
A copy of our main ETL pipeline and database building repository, but reduced in scope to only a handful of pipelines.

## Setup
First, make sure python3 and virtualenv are installed in your environment.

On a Mac with Homebrew, we can do this by running the following commands:
```sh
brew install python3
pip install virtualenv
```

On Ubuntu, we can run:
```sh
sudo apt install python3-virtualenv
```

Next, we'll need to ensure that MongoDB is running in our environment. Follow these instructions, depending on your operating system:
* [Mac](https://docs.mongodb.com/manual/tutorial/install-mongodb-on-os-x/)
* [Windows](https://docs.mongodb.com/manual/tutorial/install-mongodb-on-windows/)
* [Linux/Ubuntu](https://docs.mongodb.com/manual/tutorial/install-mongodb-on-ubuntu/)

We will also need GDAL is installed in our environment. Follow these instructions, depending on your operating system:
* [MacOS](https://trac.osgeo.org/gdal/wiki/BuildingOnMac)
* [Windows](https://sandbox.idre.ucla.edu/sandbox/tutorials/installing-gdal-for-windows)
* [Linux/Ubuntu](https://mothergeo-py.readthedocs.io/en/latest/development/how-to/gdal-ubuntu-pkg.html#install-gdal-ogr)


Once you have the base environment set up, start by making a new virtual environment and activating it
```sh
virtualenv .env
source .env/bin/activate
```

Finally, install dependencies
```sh
pip install -r requirements.txt
```

## Usage
To extract a dataset from the internet, run the `fetch` command on the dataset:
```sh
python run.py fetch <insert-name-of-dataset>
```

To transform data from raw to clean (and thus, stage it for loading into a database), run the `clean` command on the dataset:
```sh
python run.py clean <insert-name-of-dataset>
```

To load all cleaned data into a new database, use the `new-db` command:
```sh
python run.py new-db -l
```
By default, a random database name is generated. To specify a name, use `-db`:
```sh
python run.py new-db -l -db my-new-database
```

To unload and load a specific dataset in a pre-built database, use `refresh`:
```sh
python run.py refresh <insert-name-of-dataset> -l -db <insert-name-of-database>
```

To run a helper/utility script in the `app/helpers` directory, use the `helper` command:
```sh
python run.py helper <insert-name-of-helper-script>
```
