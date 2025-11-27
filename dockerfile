# Stage 1: Build JAR
FROM eclipse-temurin:17-jdk-jammy AS build
WORKDIR /app
COPY pom.xml mvnw ./
COPY src ./src
RUN ./mvnw clean package -DskipTests

# Stage 2: Create runtime image
FROM eclipse-temurin:17-jre-jammy
WORKDIR /app
COPY --from=build /app/target/hr-import-service-1.0.0.jar app.jar

ENTRYPOINT ["java", "-jar", "app.jar"]
