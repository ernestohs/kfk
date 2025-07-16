FROM eclipse-temurin:17-jdk as build

WORKDIR /app

COPY . /app
RUN ./mvnw clean package

FROM eclipse-temurin:17-jdk
WORKDIR /app
COPY --from=build /app/target/*.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]

