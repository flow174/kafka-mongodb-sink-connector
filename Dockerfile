FROM adoptopenjdk/openjdk11:x86_64-ubuntu-jdk-11.28

MAINTAINER Beck.Zhang

ENV SERVICE_HOME=/home/beck

RUN mkdir -p ${SERVICE_HOME}

COPY ./target/*.jar ${SERVICE_HOME}/app.jar
COPY ./target/lib/* ${SERVICE_HOME}/lib/
COPY config/connector.properties ${SERVICE_HOME}/
COPY config/kafka.properties ${SERVICE_HOME}/
COPY entrypoint.sh ${SERVICE_HOME}/

RUN chmod +x ${SERVICE_HOME}/entrypoint.sh

expose 8083

WORKDIR	${SERVICE_HOME}

ENTRYPOINT ["./entrypoint.sh"]
