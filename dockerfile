FROM openjdk:11-jre-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    fontconfig fonts-dejavu-extra \
    && rm -rf /var/lib/apt/lists/*

VOLUME /tmp

ARG JAR_FILE=target/hr-import-service-1.0.0.jar
COPY ${JAR_FILE} app.jar

RUN groupadd -r spring && useradd -r -g spring spring
USER spring

ENTRYPOINT ["java", "-Djava.security.egd=file:/dev/./urandom", "-jar", "/app.jar"]