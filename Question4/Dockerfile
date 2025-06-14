FROM ubuntu:latest
FROM python:3.9-slim

# Set environment variables
ENV JAVA_VERSION=17 \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Update package list and install dependencies
RUN apt-get update && apt-get install -y openjdk-17-jdk \
    && rm -rf /var/lib/apt/lists/*

# Verify Java installation
RUN java -version

ENV PATH="$JAVA_HOME/bin:$PATH"

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["spark-submit", "--master", "local[*]", "question4.py"]