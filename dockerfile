FROM eclipse-temurin:17-jdk

ENV VIRTUAL_ENV=/opt/venv
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      bash \
      nano \
      build-essential \
      postgresql-client \
      python3 \
      python3-dev \
      python3-pip \
      python3-venv \
      libffi-dev \
      libopenblas-dev \
      zlib1g-dev \
      libjpeg-dev \
      libzmq3-dev && \
    rm -rf /var/lib/apt/lists/*

RUN python3 -m venv $VIRTUAL_ENV && \
    pip install --upgrade pip setuptools wheel && \
    pip install \
      numpy \
      matplotlib \
      seaborn \
      pyspark \
      pytest \
      notebook \
      findspark && \
    ln -s "$(python -c 'import pyspark, os; print(os.path.dirname(pyspark.__file__))')" /opt/spark

RUN mkdir -p /opt/spark/conf && \
    printf '%s\n' \
      'status = error' \
      'name = SparkLog4j2Properties' \
      'rootLogger.level = error' \
      'rootLogger.appenderRefs = console' \
      'rootLogger.appenderRef.console.ref = console' \
      'appender.console.type = Console' \
      'appender.console.name = console' \
      'appender.console.target = SYSTEM_ERR' \
      'appender.console.layout.type = PatternLayout' \
      'appender.console.layout.pattern = %d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n' \
      'logger.spark.name = org.apache.spark' \
      'logger.spark.level = error' \
      'logger.spark_project.name = org.sparkproject' \
      'logger.spark_project.level = error' \
      'logger.hadoop.name = org.apache.hadoop' \
      'logger.hadoop.level = error' \
      > /opt/spark/conf/log4j2.properties

ENV SPARK_HOME=/opt/spark
ENV SPARK_CONF_DIR=/opt/spark/conf
ENV SPARK_HOME=/opt/spark

WORKDIR /src
COPY . /src/