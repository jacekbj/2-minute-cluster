FROM python:2.7.11

RUN useradd sparker

RUN mkdir /spark
RUN wget -O /tmp/spark.tgz http://d3kbcqa49mib13.cloudfront.net/spark-2.0.1-bin-hadoop2.6.tgz \
    && cd /tmp && tar zxvf spark.tgz && rm spark.tgz && cp -r spark-2.0.1-bin-hadoop2.6/* /spark/ && chown sparker:sparker -R /spark

RUN apt-get -y update && apt-get -y install openjdk-7-jdk  git htop libatlas-base-dev gfortran libevent-dev libpng-dev libfreetype6-dev libzmq-dev vim libffi-dev && apt-get -y autoremove
ADD requirements.txt /tmp/requirements.txt
RUN pip install -U pip -U setuptools && pip install numpy && pip install -r /tmp/requirements.txt && rm /tmp/requirements.txt

ENV PYTHONPATH=/src

RUN mkdir /src
ADD src/ /src

RUN mkdir /data
