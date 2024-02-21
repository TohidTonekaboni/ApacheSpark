FROM apache/spark:3.5.0 AS build
user root
COPY requirements.txt /requirements.txt

RUN pip3 install --disable-pip-version-check -r /requirements.txt 
RUN mkdir -p /home/spark && chown -R spark:spark /home/spark

user spark
WORKDIR /opt/spark-notebooks

EXPOSE 8888

ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=jupyter
ENV PYSPARK_DRIVER_PYTHON_OPTS='notebook'

ENTRYPOINT jupyter notebook --allow-root --ip 0.0.0.0 --no-browser
