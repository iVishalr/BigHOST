FROM python:3.10

RUN apt update -y
RUN apt install vim python3-pip libpq-dev python3-dev -y \
    && pip3 install --upgrade pip

USER root

COPY src /home/root/src/
# COPY scripts /home/root/scripts/
COPY solutions /home/root/solutions/
COPY requirements.txt /home/root/requirements.txt

RUN pip3 install rq==1.10.1
RUN pip3 install --no-deps -r /home/root/requirements.txt

ENV RQ_UTILS_LOCATION=/usr/local/lib/python3.10/site-packages/rq/utils.py
COPY docker/rq-utils.py $RQ_UTILS_LOCATION

WORKDIR /home/root/src/
CMD ["rq", "worker"]