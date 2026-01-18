FROM python:3.12-slim
WORKDIR /app

# deps
COPY services/options_api/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# sdk as editable
COPY sdk /sdk
RUN pip install --no-cache-dir -e /sdk

# app
COPY services/options_api/app /app/app

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
