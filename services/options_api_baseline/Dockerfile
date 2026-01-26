FROM python:3.12-slim
WORKDIR /app

COPY services/options_api_baseline/requirements.txt /app/requirements.txt

RUN pip install --no-cache-dir -r /app/requirements.txt


COPY services/options_api_baseline/app /app/app

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
