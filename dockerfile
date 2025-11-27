FROM eclipse-temurin:17-jdk-slim AS build
WORKDIR /app
COPY pom.xml mvnw ./
COPY src ./src
RUN ./mvnw clean package -DskipTests

FROM eclipse-temurin:17-jre-slim
WORKDIR /app
COPY --from=build /app/target/hr-import-service-1.0.0.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]
