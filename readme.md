# Introduce

This project used to save kafka data into mongodb.

# How to configure

You can find this project configuration in the config directory:

    |config
    |-- connector.properties
    |-- kafka.properties

Each configuration items are described in the file, please view them in the file.

# How to use?

## Build Project

    mvn clean package

## Build Docker

    ./build.sh

## Run Docker

    docker run -d -p 8083:8083 beck/kafka-mongodb-sink-connector:1.0