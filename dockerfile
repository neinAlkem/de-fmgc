FROM apache/airflow:3.1.1

USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         default-jdk \
         build-essential \
         libpq-dev \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

  ENV JAVA_HOME=/user/lib/jvm/default-java 
  USER airflow
  COPY requirements.txt /requirements.txt

  RUN pip install --no-cache-dir -r /requirements.txt