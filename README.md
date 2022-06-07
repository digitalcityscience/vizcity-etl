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