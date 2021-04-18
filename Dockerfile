FROM python:3

ENV PYTHONUNBUFFERED 1
ENV PYTHONPATH "${PYTHONPATH}:/code/"
RUN mkdir /code
WORKDIR /code
RUN mkdir /src
COPY ./src/ /code/src
RUN pip install -r ./src/requirements.txt
