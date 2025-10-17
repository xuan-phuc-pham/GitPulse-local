FROM apache/airflow:3.0.6

RUN pip install apache-airflow-providers-apache-spark pyspark s3fs pyarrow

USER root
# Install OpenJDK-17
RUN apt update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get install -y ant && \
    apt-get clean;

# Set JAVA_HOME
ENV SPARK_HOME="/home/sparkuser/spark"
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}:${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"

# Port master will be exposed
ENV SPARK_MASTER_PORT="7077"
# Name of master container and also counts as hostname
ENV SPARK_MASTER_HOST="spark-master"

RUN mkdir -p ${SPARK_HOME}
# If it breaks in this step go to https://dlcdn.apache.org/spark/ and choose higher spark version instead
RUN curl https://dlcdn.apache.org/spark/spark-3.5.6/spark-3.5.6-bin-hadoop3.tgz -o spark-3.5.6-bin-hadoop3.tgz \
    && tar xvzf spark-3.5.6-bin-hadoop3.tgz --directory ${SPARK_HOME} --strip-components 1 \
    && rm -rf spark-3.5.6-bin-hadoop3.tgz

# Download aws jar and add it to spark jars
RUN curl -L -o ${SPARK_HOME}/jars/hadoop-aws-3.3.4.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
RUN curl -L -o ${SPARK_HOME}/jars/aws-java-sdk-bundle-1.12.262.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar


RUN useradd -u 1000 -m -d /home/sparkuser sparkuser
ENV HOME="/home/sparkuser"
RUN chown -R airflow:sparkuser ${SPARK_HOME}

RUN mkdir /home/sparkuser/spark/events
RUN chmod 777 /home/sparkuser/spark/* -R

RUN apt update
RUN apt-get -y install procps

COPY ./spark-defaults.conf "${SPARK_HOME}/conf"


USER airflow
