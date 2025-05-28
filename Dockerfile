# ---------- build stage ----------
FROM maven:3.9-eclipse-temurin-21 AS builder
WORKDIR /src
COPY pom.xml ./
COPY src ./src
RUN mvn -q package -DskipTests

# ---------- run stage ----------
FROM eclipse-temurin:21-jre
# Copy the ELaaMS-app JAR from the builder stage
COPY --from=builder /src/target/ELaaMS-app-jar-with-dependencies.jar /app/ELaaMS-app.jar
ENTRYPOINT ["java","-jar","/app/ELaaMS-app.jar"]
