FROM tiangolo/uwsgi-nginx-flask:python3.6

ENV PYTHONUNBUFFERED 1
ENV PYTHONDONTWRITEBYTECODE 1
RUN mkdir -p /code
RUN mkdir -p /code/database
COPY ./requirements.txt /code/requirements.txt
RUN pip install -r /code/requirements.txt
COPY ./slave.py /code/slave.py
COPY ./master.py /code/master.py
WORKDIR /code

