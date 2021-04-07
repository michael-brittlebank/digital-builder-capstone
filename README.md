# Requirements
[Anaconda](https://www.anaconda.com/products/individual)

Generate by running
```
pipreqs
```
in the root of the project to update/generate the Requirements.txt file

# Run
```
pipenv shell
FLASK_APP=app.py FLASK_ENV=development flask run
```

# Known Issues
Curl needs to be used to ingest files as the Swagger UI will time out before the files have been fully processed

It currently takes around 10 minutes to ingest one data file
```
curl -d "type=Condo&filename=Zip_zhvi_uc_condo_tier_0.33_0.67_sm_sa_mon.csv" -X POST http://127.0.0.1:5000/files/ingest
```