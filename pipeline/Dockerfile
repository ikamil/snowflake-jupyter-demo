FROM python:3.9-slim

WORKDIR /code

# Install system dependencies and Python packages in one layer
ADD requirements.txt /tmp
RUN apt-get update \
 && apt-get install -qy \
    python3-dev \
    gfortran \
    build-essential \
 && pip install --compile -r /tmp/requirements.txt \
 && apt-get -y autoremove \
 && apt-get -y clean all \
 && rm -rf /tmp/* /var/tmp/

ADD . /code

ENV LUIGI_CONFIG_PATH=/code/tasks/luigi.conf
ENV PYTHONPATH=/code/tasks:$PYTHONPATH

ENTRYPOINT ["luigi", "--module", "etl_task", "RunPipeline", "--local-scheduler"]
