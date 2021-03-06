FROM python:3.9-alpine

LABEL org.opencontainers.image.source https://github.com/digitalcityscience/vizcity-etl

RUN apk add proj-dev proj-util musl-dev gcc

RUN adduser --home /home/app --shell /bin/bash --disabled-password app
WORKDIR /home/app
USER app

COPY requirements.txt /home/app
RUN pip install -r requirements.txt
COPY .env /home/app/
COPY *.py /home/app/


ENTRYPOINT [ "python","-m","main" ]