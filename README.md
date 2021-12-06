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
### Cron-based data maintenance

Periodic fetches and cleans can be run automatically using a scheduler like cron, with preconfigured intervals.

#### Setup

Before running anything, each dataset pipeline needs to be configured by adding a maintenance.cfg file to the root folder of the dataset in question. See `data-library/asthma/maintenance.cfg` for an example config file. Datasets that can be fetched programmatically can be configured with an interval for "fetch" ("clean" is automatically run after "fetch"), and datasets that need to be manually fetched can still be tracked by configuring an interval for "manual_fetch" - which would generate warnings when due.

#### The Cron command

After configuring the datasets as above, run the following command on a daily cron:
```sh
python run.py helper maintain
```
This command will check for any run history data and will execute any scheduled "fetch" and "manual_fetch" that are due, as well as update the run history accordingly. If an interval is configured in a maintenance.cfg but no history data is found, the operation will be assumed due and run immediately.

Run history data for each dataset is stored in a config file in the data/ folder of the dataset in question.

With this command on daily cron, barring any exceptions, all datasets that can be programmatically fetched should self-maintain without any human intervention.

#### Manual datasets

Datasets that cannot be programmatically fetched can be configured with an interval for "manual_fetch". This will generate warnings when the dataset is due for fetching, so that a human maintainer can be alerted to the need to perform a manual fetch and clean.

Upon manually fetching and cleaning, the maintainer should mark the dataset as fetched using the mark_fetched command:
```sh
python run.py mark_fetched <insert-name-of-dataset>
```
This alerts the maintenance mechanism that the data has been updated and will silence the warnings until the dataset is next due.

**Note: "fetch", "clean" and "flean" commands update the run history by default. To run these commands without affecting the run history data, use the "--nomark" flag. eg.**
```sh
python run.py fetch --nomark <insert-name-of-dataset>
```

#### Automatically keeping the code updated

**pullandrun.sh** can be run on the daily cron instead of the maintain command directly to ensure that a git pull is always executed before the maintain command itself. For this to work, make sure a **config.sh** file is available at the root folder that contains github credentials with access to the repository. A sample config file is available for reference.

#### Viewing data maintenance status

Running the following:
```sh
python run.py helper print_maintain_status text
```
Outputs a simple text report of the current state of each dataset, including the following info:
 * last successful fetch
 * next scheduled fetch
 * if the current dataset is due for fetch/manual fetch
 * last exception info, if last attempted run resulted in an exception

This can be used as a basis for a simple status dashboard that can be served on a web page.
