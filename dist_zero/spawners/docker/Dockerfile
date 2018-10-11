FROM python:3.6.5

RUN pip install pipenv==2018.5.18

RUN apt-get update
RUN apt-get install -y net-tools

RUN mkdir /machine
WORKDIR /machine

COPY Pipfile .
COPY Pipfile.lock .
RUN pipenv install
