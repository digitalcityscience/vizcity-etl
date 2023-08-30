# vizcity-etl
Extract, transform, load pipeline for the vizcity teaching course

## Why?
I am aware of [telegraf](https://www.influxdata.com/time-series-platform/telegraf/) and its rich format support. The remote sources used here however not always stick with conventions in regards of date-time formats, parameter count etc. which makes them tricky to get set up right. It is ultimately easier to control the whole [ETL](https://en.wikipedia.org/wiki/Extract,_transform,_load) process.

## How to use:
Fill out the credentials for the remote services in the environment configuration file.

Running locally:
```
  cp .env.documentation .env
  make init
  make install
  python3 -m main
```

Running in a container:
```
  cp .env.documentation .env
  make init
  make install
  make docker
  make run-docker
```

### Tools used for parsing the events
- [jmespath](https://jmespath.org) for JSON
- [xmltodict](https://github.com/martinblech/xmltodict) for XML


### DCS INFO
How do we run this? How do we use this on our local server?
- InfluxDB front-end for visualization
    - start via podman with /opt/influxdb$ sh influx.sh 


- Data Pipeline vizcity-etl
    - collects mobilty data and inserts it into influxdb
    - runs as cronjob of webapp user and appends to a logfile for each run in /var/log/vizcity-etl.log
    # m h  dom mon dow   command
    */15 * * * * date  >> /var/log/vizcity-etl.log
    */15 * * * * podman run --rm ghcr.io/digitalcityscience/vizcity-etl:latest >> /var/log/vizcity-etl.log 2>&1

    How to update the pipeline??
    - clone out https://github.com/digitalcityscience/vizcity-etl
    - make your edits
    - push to the ghcr from your local computer. 
        - login to ghcr first with docker
            - create a token to access, pull, push to ghcr in your github settings
            - export CR_PAT=ghp_EXAMPLE_TOKEN
            - echo $CR_PAT | docker login ghcr.io -u andredaa --password-stdin
        - now push/publish your changes of the image at ghcr
            - docker push ghcr.io/digitalcityscience/vizcity-etl:latest 
        
        - now login to the VM and pull the updated image from the ghcr
            - with podman!
            - podman image pul
            - (you might have to login to ghcr with podman login, see login performed with docker on your machine above)l ghcr.io/digitalcityscience/vizcity-etl:latest