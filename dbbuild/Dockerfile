FROM osgeo/gdal:ubuntu-full-3.6.3
# This image is built on "Ubuntu 22.04.2 LTS"

ENV JAGEOCODER_DB2_DIR /opt/db2

# Install the required Python packages.
# Note: python3-dev, libpq-dev is needed for building psycopg2 package.
COPY ./requirements.txt /tmp/
RUN apt-get update && apt-get install -y \
    libpq-dev \
    postgresql-client-14 \
    python3 \
    python3-dev \
    python3-pip \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*
RUN python3 -m pip install -r /tmp/requirements.txt \
    && python3 -m pip install jageocoder_converter==2.0.3

# Install Jageocoder gaiku_all_v20.zip
RUN curl https://www.info-proto.com/static/jageocoder/latest/gaiku_all_v21.zip \
    -o /opt/gaiku_all_v21.zip && \
    jageocoder install-dictionary /opt/gaiku_all_v21.zip && \
    rm /opt/gaiku_all_v21.zip

# Setup and run server
COPY ./app /app

# Run server
WORKDIR /
ENV PGHOST postgis_realestate
CMD ["/bin/bash", "/app/run.sh"]
