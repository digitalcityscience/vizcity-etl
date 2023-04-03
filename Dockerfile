FROM python:3.11.2-slim

LABEL org.opencontainers.image.source https://github.com/digitalcityscience/vizcity-etl

RUN adduser --home /home/app --shell /bin/bash --disabled-password app
WORKDIR /home/app
USER app

COPY requirements.txt /home/app
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

COPY .env /home/app/
COPY *.py /home/app/
COPY fixtures /home/app/fixtures

ENTRYPOINT [ "python","-m","main" ]