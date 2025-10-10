FROM python:3.10-slim 

WORKDIR /app

COPY /etl/etl.py .
COPY ../data /data

RUN pip install pandas psycopg2-binary

CMD [ "python", "etl.py" ]