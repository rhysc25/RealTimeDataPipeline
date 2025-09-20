FROM python:3.11-slim
WORKDIR /app

# Install Java 21 and system utilities
RUN apt-get update && \
    apt-get install -y openjdk-21-jre-headless procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME for Java 21
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY DataProcess.py .

CMD ["python", "DataProcess.py"]