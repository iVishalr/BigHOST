FROM ubuntu:22.04
# change to python base image?

RUN apt update -y
RUN apt install vim openssh-server openssh-client python3-pip libpq-dev python3-dev openjdk-8-jdk -y \
    && pip3 install --upgrade pip

USER root

COPY src /home/root/src/
# COPY scripts /home/root/scripts/
COPY solutions /home/root/solutions/
COPY requirements.txt /home/root/requirements.txt

RUN pip3 install --no-deps -r /home/root/requirements.txt

WORKDIR /home/root/src/
CMD ["python3", "main.py"]