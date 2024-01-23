FROM python:3.9

RUN pip install sqlalchemy
RUN pip install psycopg2
RUN pip install wget
RUN pip install pandas

WORKDIR /app

COPY ingest_data.py ingest_data.py

ENTRYPOINT ["python" , "ingest_data.py"]