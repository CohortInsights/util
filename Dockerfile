FROM ubuntu:22.04

RUN apt-get update
RUN apt-get install -y python3
RUN apt-get install -y pip

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

ADD util util
