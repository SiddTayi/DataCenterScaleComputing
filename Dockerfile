FROM python:3.11

WORKDIR /lab1
RUN pip install pandas

COPY pipeline.py pipeline_c.py
COPY Power.csv Power_c.csv

RUN python pipeline_c.py Power_c.csv Target.csv


ENTRYPOINT [ "bash" ]