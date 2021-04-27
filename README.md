# Digital Builder Capstone

## Development
Generate python requirements by running
```
pipreqs
```
in the root of the project to update/generate the Requirements.txt file


## Running the Code

!! Only tested in OSX !!

### Requirements
An [Anaconda](https://www.anaconda.com/products/individual) environment provides most of the requirements needed to run the project.
A few additional Pip installations may be required such as googlemaps and are listed in the Requirements.txt file at the root of the repo.

#### Environment Variables
Create a copy of the .env_sample file and name it .env

Then enter the corresponding values:
* A Google API key is needed for the Places and Location APIs in order to count AmFam agency locations
* A MySQL database user, password, and host is needed to connect to the database where ingested data is stored 

### Run the API locally
This command starts the flask server
```
FLASK_APP=app.py FLASK_ENV=development flask run
```
The Swagger documentation is now available at [http://127.0.0.1:5000/](http://127.0.0.1:5000/).
API calls can be made via the Swagger UI. See below for known issues calling the /data routes

### Known Issues
Curl needs to be used for all /data calls as the Swagger UI will time out before the request has been fully processed

It currently takes around 20-30 minutes to ingest a full data file. 
Example request (when Flask server is running):
```
curl -d "type=Condo&filename=Zip_zhvi_uc_condo_tier_0.33_0.67_sm_sa_mon.csv" -X POST http://127.0.0.1:5000/data/ingest
```

### API Workflow & Data Ingestion Process
1. POST /data/populate - Prepare the database and create the tables
2. POST /data/ingest - Import individual Zillow CSV data into the database
3. POST /data/calculate-location-metrics - Calculate location metrics
4. POST /data/calculate-yearly-zhvi - Calculate compound metrics
5. GET /analysis/baseline - Establish benchmarks by querying the calculated metrics
6. GET /graphs/trend-zhvi - Establish trends and graph the baseline data of ZHVI value over time
7. GET /data/calculate-agency-density - Find number of AmFam agencies per top performing zip codes
8. GET /analysis/leaders - Get the top performing zip codes with AmFam agency density
