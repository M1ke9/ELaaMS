# ---------- build stage ----------
FROM maven:3.9-eclipse-temurin-21 AS builder
WORKDIR /src
COPY pom.xml ./
COPY src ./src
RUN mvn -q package -DskipTests

# ---------- run stage ----------
FROM eclipse-temurin:21-jre
COPY --from=builder /src/target/router-app-jar-with-dependencies.jar /app/router-app.jar
ENTRYPOINT ["java","-jar","/app/router-app.jar"]
