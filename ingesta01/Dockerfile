FROM python:3-slim

WORKDIR /programas/ingesta

RUN pip3 install --no-cache-dir boto3 mysql-connector-python faker

COPY . .

CMD ["python3", "./ingesta.py"]
