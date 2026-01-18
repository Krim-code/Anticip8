import os, random
from locust import HttpUser, task, between

ORDERS = os.getenv("ORDERS_BASE_URL", "http://orders-api:8000")
OPTIONS = os.getenv("OPTIONS_BASE_URL", "http://options-api:8000")

class UserFlow(HttpUser):
    wait_time = between(0.2, 0.8)

    def on_start(self):
        self.user_key = f"u{random.randint(1, 200)}"

    @task(5)
    def flow_orders_to_options(self):
        # /orders -> /orders/{id} -> /order-options/{id}
        self.client.get(f"{ORDERS}/orders", headers={"x-user": self.user_key}, name="/orders")

        oid = random.randint(1, 20)
        self.client.get(f"{ORDERS}/orders/{oid}", headers={"x-user": self.user_key}, name="/orders/{id}")

        # цель: этот эндпоинт будет "дорогим", и мы хотим увидеть как prefetch помогает дальше (потом добавим кэш)
        self.client.get(f"{OPTIONS}/order-options/{oid}", headers={"x-user": self.user_key}, name="/order-options/{id}")

    @task(1)
    def sometimes_contacts(self):
        self.client.get(f"{OPTIONS}/contacts", headers={"x-user": self.user_key}, name="/contacts")
