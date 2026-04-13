FROM apache/airflow:2.9.2

USER airflow

RUN pip install --no-cache-dir \
    pytrends==4.9.2 \
    yfinance==1.2.1 \
    pandas==2.2.2 \
    psycopg2-binary==2.9.9