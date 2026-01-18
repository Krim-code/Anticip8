FROM python:3.12-slim
WORKDIR /locust
RUN pip install --no-cache-dir locust==2.31.8
COPY locustfile.py .
CMD ["locust", "-f", "locustfile.py", "--host=http://orders-api:8000", "--web-host=0.0.0.0"]
