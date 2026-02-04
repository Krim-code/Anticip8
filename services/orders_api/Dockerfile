FROM python:3.12-slim
WORKDIR /app

COPY services/orders_api/requirements.txt /app/requirements.txt

RUN pip install --no-cache-dir -r /app/requirements.txt

COPY sdk /sdk
RUN pip install --no-cache-dir -e /sdk

COPY services/orders_api/app /app/app

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000" ]
