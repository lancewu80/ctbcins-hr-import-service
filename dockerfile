# Stage 1: Build JAR
FROM eclipse-temurin:17-jdk-jammy AS build
WORKDIR /app

# Install Maven
RUN apt-get update && \
    apt-get install -y maven && \
    rm -rf /var/lib/apt/lists/*

COPY pom.xml ./ 
COPY src ./src

RUN mvn clean package -DskipTests


# Stage 2: Create runtime image
FROM eclipse-temurin:17-jre-jammy
WORKDIR /app
COPY --from=build /app/target/hr-import-service-1.0.0.jar app.jar

ENTRYPOINT ["java", "-jar", "app.jar"]

