import os, random
from locust import HttpUser, task, between

ORDERS = os.getenv("ORDERS_BASE_URL", "http://orders-api:8000")
OPTIONS = os.getenv("OPTIONS_BASE_URL", "http://options-api:8000")

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
        self.user_key = f"u{random.randint(1, 500)}"
        self.last_order_id = random.randint(1, 200)
        self.last_customer_id = random.randint(1, 200)
        self.last_product_id = random.randint(1, 200)

    # ----- helpers -----
    def pick_order(self):
        # расширяем id, чтобы не вариться только в 1..20
        self.last_order_id = random.randint(1, 500)
        return self.last_order_id

    def pick_customer(self):
        self.last_customer_id = random.randint(1, 200)
        return self.last_customer_id

    def pick_product(self):
        self.last_product_id = random.randint(1, 400)
        return self.last_product_id


class ShopperFlow(BaseFlow):
    """
    Профиль 1: “покупатель”
    Главная -> каталог -> продукт -> отзывы -> промо/рек -> корзина -> профиль
    """

    @task(4)
    def home_feed(self):
        self.client.get(f"{ORDERS}/feed", headers=h(self.user_key), name="/feed")

    @task(6)
    def browse_catalog(self):
        cat = random.choice(CATEGORIES)
        page = random.randint(1, 5)
        q = random.choice(SEARCH_Q) if random.random() < 0.35 else ""
        self.client.get(
            f"{OPTIONS}/catalog/products?category={cat}&page={page}&q={q}",
            headers=h(self.user_key),
            name="/catalog/products?category={c}&page={p}&q={q}",
        )

        pid = self.pick_product()
        self.client.get(f"{OPTIONS}/catalog/products/{pid}", headers=h(self.user_key), name="/catalog/products/{id}")

        if random.random() < 0.55:
            self.client.get(
                f"{OPTIONS}/catalog/products/{pid}/reviews?page={random.randint(1,3)}",
                headers=h(self.user_key),
                name="/catalog/products/{id}/reviews?page={p}",
            )

        if random.random() < 0.35:
            self.client.get(f"{OPTIONS}/promotions/active", headers=h(self.user_key), name="/promotions/active")

        if random.random() < 0.35:
            self.client.get(f"{OPTIONS}/recommendations?user_id={self.user_key}", headers=h(self.user_key), name="/recommendations")

    @task(3)
    def basket_and_profile(self):
        self.client.get(f"{ORDERS}/basket/summary?user_id={self.user_key}", headers=h(self.user_key), name="/basket/summary")
        if random.random() < 0.6:
            self.client.get(f"{ORDERS}/basket/items?user_id={self.user_key}", headers=h(self.user_key), name="/basket/items")
        if random.random() < 0.4:
            self.client.get(f"{ORDERS}/profile?user_id={self.user_key}", headers=h(self.user_key), name="/profile")
            self.client.get(f"{ORDERS}/profile/history?user_id={self.user_key}&page={random.randint(1,4)}",
                            headers=h(self.user_key), name="/profile/history?page={p}")


class OrderManagerFlow(BaseFlow):
    """
    Профиль 2: “менеджер заказов”
    /orders (с фильтрами) -> order -> items/status/payment -> options/pricing/delivery/timeline -> customer+addresses
    """

    @task(8)
    def deep_order_flow(self):
        status = random.choice(STATUSES)
        page = random.randint(1, 6)
        q = random.choice(SEARCH_Q) if random.random() < 0.2 else ""
        self.client.get(
            f"{ORDERS}/orders?status={status}&page={page}&q={q}",
            headers=h(self.user_key),
            name="/orders?status={s}&page={p}&q={q}",
        )

        oid = self.pick_order()
        self.client.get(f"{ORDERS}/orders/{oid}", headers=h(self.user_key), name="/orders/{id}")

        # ветвления внутри сервиса
        if random.random() < 0.8:
            self.client.get(f"{ORDERS}/orders/{oid}/items", headers=h(self.user_key), name="/orders/{id}/items")
        if random.random() < 0.7:
            self.client.get(f"{ORDERS}/orders/{oid}/status", headers=h(self.user_key), name="/orders/{id}/status")
        if random.random() < 0.55:
            self.client.get(f"{ORDERS}/orders/{oid}/payment", headers=h(self.user_key), name="/orders/{id}/payment")
        if random.random() < 0.12:
            reason = random.choice(REASONS)
            self.client.get(f"{ORDERS}/orders/{oid}/cancel?reason={reason}", headers=h(self.user_key), name="/orders/{id}/cancel?reason={r}")

        # кросс-сервисные запросы (то, ради чего префетч)
        if random.random() < 0.85:
            self.client.get(f"{OPTIONS}/order-options/{oid}", headers=h(self.user_key), name="/order-options/{id}")
        if random.random() < 0.55:
            self.client.get(f"{OPTIONS}/orders/{oid}/pricing", headers=h(self.user_key), name="/orders/{id}/pricing")
        if random.random() < 0.45:
            self.client.get(f"{OPTIONS}/orders/{oid}/delivery", headers=h(self.user_key), name="/orders/{id}/delivery")
        if random.random() < 0.4:
            self.client.get(f"{OPTIONS}/orders/{oid}/timeline", headers=h(self.user_key), name="/orders/{id}/timeline")

        # ещё одна “ветка” (customer)
        if random.random() < 0.5:
            cid = self.pick_customer()
            self.client.get(f"{OPTIONS}/customers/{cid}", headers=h(self.user_key), name="/customers/{id}")
            if random.random() < 0.75:
                self.client.get(f"{OPTIONS}/customers/{cid}/addresses", headers=h(self.user_key), name="/customers/{id}/addresses")


class SupportFlow(BaseFlow):
    """
    Профиль 3: “поддержка”
    tickets -> ticket -> contacts -> profile/search
    """

    @task(5)
    def support_journey(self):
        status = random.choice(["open", "closed", "pending"])
        self.client.get(f"{OPTIONS}/support/tickets?user_id={self.user_key}&status={status}",
                        headers=h(self.user_key), name="/support/tickets?status={s}")

        tid = random.randint(1, 50)
        self.client.get(f"{OPTIONS}/support/tickets/{tid}", headers=h(self.user_key), name="/support/tickets/{id}")

        if random.random() < 0.65:
            self.client.get(f"{OPTIONS}/contacts", headers=h(self.user_key), name="/contacts")

        if random.random() < 0.4:
            self.client.get(f"{ORDERS}/profile?user_id={self.user_key}", headers=h(self.user_key), name="/profile")

        if random.random() < 0.4:
            q = random.choice(SEARCH_Q)
            self.client.get(f"{ORDERS}/search?q={q}&limit={random.randint(10,30)}", headers=h(self.user_key), name="/search?q={q}&limit={l}")
