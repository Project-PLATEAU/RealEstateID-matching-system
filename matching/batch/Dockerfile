FROM --platform=linux/x86_64 ghcr.io/osgeo/gdal:ubuntu-small-3.7.1

RUN apt-get update && \
    apt-get install -y postgresql-client && \
    apt-get install -y python3-pip
RUN pip install boto3 psycopg2-binary install python-dotenv lxml requests pytz

WORKDIR /app

RUN mkdir /app/data

COPY src /app/src







