import asyncio
import json
from typing import Any

import httpx
from openpyxl import Workbook


QUERY = "пальто из натуральной шерсти"
SEARCH_URL = "https://search.wb.ru/exactmatch/ru/common/v4/search"
DETAIL_URL = "https://card.wb.ru/cards/v1/detail"

TRANSIENT = {429, 500, 502, 503, 504}


def money(v: Any) -> float | None:
    try:
        return float(v) / 100.0 if v is not None else None
    except (TypeError, ValueError):
        return None


def product_url(nm_id: int | None) -> str | None:
    return f"https://www.wildberries.ru/catalog/{nm_id}/detail.aspx" if nm_id else None


def seller_url(supplier_id: int | None) -> str | None:
    return f"https://www.wildberries.ru/seller/{supplier_id}" if supplier_id else None


def sizes(p: dict) -> str:
    sizes = []
    for sz in p.get("sizes") or []:
        s = sz.get("name") or sz.get("origName")
        if s:
            sizes.append(str(s))
    return ", ".join(sizes)


def stocks(p: dict) -> int:
    total = 0
    for sz in p.get("sizes") or []:
        for st in sz.get("stocks") or []:
            try:
                total += int(st.get("qty") or 0)
            except (TypeError, ValueError):
                pass
    return total


def images(p: dict) -> str:
    nm_id = p.get("id") or p.get("nmId")
    pics = p.get("pics")
    try:
        nm_id = int(nm_id)
        pics = int(pics)
    except (TypeError, ValueError):
        return ""
    basket = nm_id // 100000
    host = f"https://basket-{basket:02d}.wbbasket.ru"
    links = [
        f"{host}/vol{nm_id // 100000}/part{nm_id // 1000}/{nm_id}/images/big/{i}.jpg"
        for i in range(1, pics + 1)
    ]
    return ", ".join(links)


def has_russia(x: Any) -> bool:
    if isinstance(x, dict):
        for k, v in x.items():
            if "страна" in str(k).lower() and "россия" in str(v).lower():
                return True
            if has_russia(v):
                return True
        return False
    if isinstance(x, list):
        return any(has_russia(v) for v in x)
    return "россия" in str(x).lower()


class WBClient:
    def __init__(self, http: httpx.AsyncClient, sem: asyncio.Semaphore | None = None, max_attempts: int = 5, retry_delay: float = 2.0):
        self.http = http
        self.sem = sem
        self.max_attempts = max_attempts
        self.retry_delay = retry_delay

    async def get_json(self, url: str, params: dict[str, Any]) -> dict:
        for attempt in range(1, self.max_attempts + 1):
            try:
                if self.sem:
                    async with self.sem:
                        r = await self.http.get(url, params=params)
                else:
                    r = await self.http.get(url, params=params)
            except httpx.HTTPError as e:
                if attempt != self.max_attempts:
                    await asyncio.sleep(self.retry_delay)
                    continue
                raise

            if r.status_code == 200:
                data = r.json()
                if data:
                    return data
                raise RuntimeError("200 but empty json")

            if r.status_code in TRANSIENT and attempt != self.max_attempts:
                await asyncio.sleep(self.retry_delay)
                continue

            raise RuntimeError(f"request failed: {r.status_code} {r.text}")

        raise RuntimeError("unreachable")


async def fetch_all_nm_ids(client: WBClient, limit: int = 100) -> list[int]:
    page = 1
    nm_ids: list[int] = []

    while True:
        params = {
            "appType": 1,
            "curr": "rub",
            "dest": -1257786,
            "lang": "ru",
            "locale": "ru",
            "query": QUERY,
            "page": page,
            "resultset": "catalog",
            "sort": "popular",
            "spp": 0,
            "limit": limit,
        }
        data = await client.get_json(SEARCH_URL, params=params)
        products = (((data.get("data") or {}).get("products")) or [])
        if not products:
            break

        for p in products:
            nm = p.get("id") or p.get("nmId")
            try:
                nm_ids.append(int(nm))
            except (TypeError, ValueError):
                pass

        page += 1

    return nm_ids


async def fetch_detail(client: WBClient, nm_id: int) -> dict | None:
    params = {"appType": 1, "curr": "rub", "dest": -1257786, "spp": 0, "nm": nm_id}
    data = await client.get_json(DETAIL_URL, params=params)
    products = (((data.get("data") or {}).get("products")) or [])
    return products[0] if products else None


def parse_row(p: dict) -> list[Any]:
    nm_id = p.get("id") or p.get("nmId")
    supplier_id = p.get("supplierId")

    price = money(p.get("salePriceU")) or money(p.get("priceU")) or money(p.get("price"))
    descr = p.get("description") or p.get("descr")
    seller_name = p.get("supplier") or p.get("supplierName")

    rating = None
    try:
        if p.get("rating") is not None:
            rating = float(p.get("rating"))
    except (TypeError, ValueError):
        pass

    reviews = None
    for k in ("feedbacks", "feedbacksCount", "reviews", "commentsCount"):
        if p.get(k) is not None:
            try:
                reviews = int(p.get(k))
                break
            except (TypeError, ValueError):
                pass

    characteristics = p.get("properties") or p.get("options") or p.get("characteristics") or {}
    characteristics_json = json.dumps(characteristics, ensure_ascii=False, indent=2)

    return [
        product_url(int(nm_id)) if nm_id else None,
        int(nm_id) if nm_id else None,
        p.get("name"),
        price,
        descr,
        images(p),
        characteristics_json,
        seller_name,
        seller_url(int(supplier_id)) if supplier_id else None,
        sizes(p),
        stocks(p),
        rating,
        reviews,
    ]


def write_xlsx(path: str, rows: list[list[Any]]) -> None:
    wb = Workbook()
    ws = wb.active
    ws.title = "catalog"

    ws.append([
        "Ссылка на товар",
        "Артикул",
        "Название",
        "Цена",
        "Описание",
        "Ссылки на изображения",
        "Все характеристики (json)",
        "Название селлера",
        "Ссылка на селлера",
        "Размеры",
        "Остатки",
        "Рейтинг",
        "Количество отзывов",
    ])

    for r in rows:
        ws.append(r)

    wb.save(path)


async def main() -> None:
    sem = asyncio.Semaphore(12)

    async with httpx.AsyncClient(timeout=30.0, headers={"User-Agent": "Mozilla/5.0"}) as http:
        client = WBClient(http=http, sem=sem)

        nm_ids = await fetch_all_nm_ids(client, limit=100)
        details = await asyncio.gather(*[fetch_detail(client, nm) for nm in nm_ids])

        full_rows: list[list[Any]] = []
        for d in details:
            if d:
                full_rows.append(parse_row(d))

        write_xlsx("wb_catalog_full.xlsx", full_rows)

        filtered_rows: list[list[Any]] = []
        for r in full_rows:
            price = r[3]
            rating = r[11]
            try:
                ch = json.loads(r[6] or "{}")
            except Exception:
                continue

            if rating is None or price is None:
                continue
            if float(rating) < 4.5:
                continue
            if float(price) > 10000:
                continue
            if not has_russia(ch):
                continue

            filtered_rows.append(r)

        write_xlsx("wb_catalog_filtered.xlsx", filtered_rows)


if __name__ == "__main__":
    asyncio.run(main())
