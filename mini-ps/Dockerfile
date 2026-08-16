# ==========================================
# STAGE 1: Biên dịch ứng dụng (Build Stage)
# ==========================================
FROM maven:3.9.6-eclipse-temurin-21-alpine AS builder

WORKDIR /app

# Sao chép file cấu hình pom.xml từ thư mục gốc hiện tại vào container
COPY pom.xml .
RUN mvn dependency:go-offline -B

# Sao chép thư mục src từ thư mục gốc hiện tại vào container
COPY src ./src

# Biên dịch và đóng gói ra file JAR
RUN mvn clean package -DskipTests

# ==========================================
# STAGE 2: Khởi chạy ứng dụng (Runtime Stage)
# ==========================================
FROM eclipse-temurin:21-jre-alpine

WORKDIR /app

RUN addgroup -S minipsgroup && adduser -S minipsuser -G minipsgroup

# Sao chép file JAR đã build từ Stage 1 sang Stage 2
COPY --from=builder /app/target/minipubsub-server-*.jar app.jar

RUN chown minipsuser:minipsgroup app.jar
USER minipsuser

EXPOSE ${SERVER_PORT}

ENTRYPOINT ["java", "-XX:+UseG1GC", "-XX:MaxRAMPercentage=75.0", "-Djava.security.egd=file:/dev/./urandom", "-jar", "app.jar"]