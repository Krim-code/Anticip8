import os, random
from locust import HttpUser, task, between

ORDERS = os.getenv("ORDERS_BASE_URL", "http://orders-api:8000")
OPTIONS = os.getenv("OPTIONS_BASE_URL", "http://options-api:8000")

USERS = int(os.getenv("USERS", "10"))

# рабочие пулы (для демо hit-rate)
ORDER_POOL = int(os.getenv("ORDER_POOL", "60"))
PRODUCT_POOL = int(os.getenv("PRODUCT_POOL", "80"))
CUSTOMER_POOL = int(os.getenv("CUSTOMER_POOL", "50"))

STATUSES = ["new", "paid", "packed", "shipped", "delivered", "canceled"]
REASONS = ["changed_mind", "wrong_address", "too_expensive", "late_delivery"]
CATEGORIES = [1, 2, 3, 4, 5]
SEARCH_Q = ["neo", "trinity", "matrix", "order", "product", "customer", "promo", "sale"]

def h(user_key: str):
    return {"x-user": user_key}

class BaseFlow(HttpUser):
    abstract = True
    wait_time = between(0.15, 0.7)

    def on_start(self):
        self.user_key = f"u{random.randint(1, USERS)}"
        # “липкие” id: пользователь чаще крутится вокруг своих
        self.last_order_id = random.randint(1, ORDER_POOL)
        self.last_customer_id = random.randint(1, CUSTOMER_POOL)
        self.last_product_id = random.randint(1, PRODUCT_POOL)

    def pick_order(self):
        if random.random() < 0.65:
            return self.last_order_id
        self.last_order_id = random.randint(1, ORDER_POOL)
        return self.last_order_id

    def pick_customer(self):
        if random.random() < 0.65:
            return self.last_customer_id
        self.last_customer_id = random.randint(1, CUSTOMER_POOL)
        return self.last_customer_id

    def pick_product(self):
        if random.random() < 0.65:
            return self.last_product_id
        self.last_product_id = random.randint(1, PRODUCT_POOL)
        return self.last_product_id


class ShopperFlow(BaseFlow):
    @task(4)
    def home_feed(self):
        # user-scoped: без query
        self.client.get(f"{ORDERS}/feed", headers=h(self.user_key), name="/feed")
        # повтор для хитов
        if random.random() < 0.35:
            self.client.get(f"{ORDERS}/feed", headers=h(self.user_key), name="/feed (repeat)")

    @task(6)
    def browse_catalog(self):
        cat = random.choice(CATEGORIES)
        page = random.randint(1, 3)  # меньше вариантов
        q = random.choice(SEARCH_Q) if random.random() < 0.2 else ""
        self.client.get(
            f"{OPTIONS}/catalog/products?category={cat}&page={page}&q={q}",
            headers=h(self.user_key),
            name="/catalog/products?category={c}&page={p}&q={q}",
        )

        pid = self.pick_product()
        self.client.get(f"{OPTIONS}/catalog/products/{pid}", headers=h(self.user_key), name="/catalog/products/{id}")

        if random.random() < 0.45:
            p = random.randint(1, 2)
            self.client.get(
                f"{OPTIONS}/catalog/products/{pid}/reviews?page={p}",
                headers=h(self.user_key),
                name="/catalog/products/{id}/reviews?page={p}",
            )

        if random.random() < 0.25:
            self.client.get(f"{OPTIONS}/promotions/active", headers=h(self.user_key), name="/promotions/active")

        if random.random() < 0.25:
            self.client.get(f"{OPTIONS}/recommendations", headers=h(self.user_key), name="/recommendations")

    @task(4)
    def basket_and_profile(self):
        # user-scoped: без user_id в query
        self.client.get(f"{ORDERS}/basket/summary", headers=h(self.user_key), name="/basket/summary")
        if random.random() < 0.65:
            self.client.get(f"{ORDERS}/basket/items", headers=h(self.user_key), name="/basket/items")

        if random.random() < 0.55:
            self.client.get(f"{ORDERS}/profile", headers=h(self.user_key), name="/profile")
            p = random.randint(1, 2)
            self.client.get(f"{ORDERS}/profile/history?page={p}", headers=h(self.user_key), name="/profile/history?page={p}")

        # повтор для хитов (после прогрева)
        if random.random() < 0.35:
            self.client.get(f"{ORDERS}/basket/summary", headers=h(self.user_key), name="/basket/summary (repeat)")


class OrderManagerFlow(BaseFlow):
    @task(8)
    def deep_order_flow(self):
        status = random.choice(STATUSES)
        page = random.randint(1, 3)
        q = random.choice(SEARCH_Q) if random.random() < 0.1 else ""
        self.client.get(
            f"{ORDERS}/orders?status={status}&page={page}&q={q}",
            headers=h(self.user_key),
            name="/orders?status={s}&page={p}&q={q}",
        )

        oid = self.pick_order()
        self.client.get(f"{ORDERS}/orders/{oid}", headers=h(self.user_key), name="/orders/{id}")

        if random.random() < 0.85:
            self.client.get(f"{ORDERS}/orders/{oid}/items", headers=h(self.user_key), name="/orders/{id}/items")
        if random.random() < 0.55:
            self.client.get(f"{ORDERS}/orders/{oid}/payment", headers=h(self.user_key), name="/orders/{id}/payment")

        # кросс-сервис
        if random.random() < 0.85:
            self.client.get(f"{OPTIONS}/order-options/{oid}", headers=h(self.user_key), name="/order-options/{id}")

        # повторяем ключевые ручки ради хитов
        if random.random() < 0.35:
            self.client.get(f"{ORDERS}/orders/{oid}/items", headers=h(self.user_key), name="/orders/{id}/items (repeat)")


class SupportFlow(BaseFlow):
    @task(5)
    def support_journey(self):
        status = random.choice(["open", "closed", "pending"])
        self.client.get(
            f"{OPTIONS}/support/tickets?status={status}",
            headers=h(self.user_key),
            name="/support/tickets?status={s}",
        )

        tid = random.randint(1, 30)
        self.client.get(f"{OPTIONS}/support/tickets/{tid}", headers=h(self.user_key), name="/support/tickets/{id}")

        if random.random() < 0.65:
            self.client.get(f"{OPTIONS}/contacts", headers=h(self.user_key), name="/contacts")

        if random.random() < 0.45:
            self.client.get(f"{ORDERS}/profile", headers=h(self.user_key), name="/profile")
