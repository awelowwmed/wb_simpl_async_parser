import asyncio
import json
import random
from typing import Any

import httpx
from openpyxl import Workbook


QUERY = "пальто из натуральной шерсти"
SEARCH_URL = "https://search.wb.ru/exactmatch/ru/common/v4/search"
DETAIL_URL = "https://card.wb.ru/cards/detail"

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
    out = []
    for sz in p.get("sizes") or []:
        s = sz.get("name") or sz.get("origName")
        if s:
            out.append(str(s))
    return ", ".join(out)


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
    def __init__(
        self,
        http: httpx.AsyncClient,
        sem: asyncio.Semaphore | None = None,
        max_attempts: int = 10,
        base_delay: float = 1.5,
        max_delay: float = 60.0,
    ):
        self.http = http
        self.sem = sem
        self.max_attempts = max_attempts
        self.base_delay = base_delay
        self.max_delay = max_delay

    def _retry_after(self, r: httpx.Response) -> float | None:
        ra = r.headers.get("Retry-After")
        if not ra:
            return None
        try:
            return float(ra)
        except (TypeError, ValueError):
            return None

    async def _sleep_backoff(self, attempt: int, retry_after: float | None = None) -> None:
        if retry_after is not None:
            delay = retry_after
        else:
            delay = min(self.max_delay, self.base_delay * (2 ** (attempt - 1)))
            delay = delay + random.uniform(0, 1.2)
        await asyncio.sleep(delay)

    async def get_json(self, url: str, params: dict[str, Any]) -> dict:
        last_err: str | None = None

        for attempt in range(1, self.max_attempts + 1):
            try:
                if self.sem:
                    async with self.sem:
                        r = await self.http.get(url, params=params)
                else:
                    r = await self.http.get(url, params=params)
            except httpx.HTTPError as e:
                last_err = f"http error: {e}"
                if attempt != self.max_attempts:
                    await self._sleep_backoff(attempt)
                    continue
                raise

            if r.status_code == 200:
                data = r.json()
                if data:
                    return data
                last_err = "200 but empty json"
                if attempt != self.max_attempts:
                    await self._sleep_backoff(attempt)
                    continue
                raise RuntimeError(last_err)

            if r.status_code in TRANSIENT and attempt != self.max_attempts:
                last_err = f"{r.status_code} {r.text}"
                ra = self._retry_after(r) if r.status_code == 429 else None
                await self._sleep_backoff(attempt, retry_after=ra)
                continue
            
            raise RuntimeError(f"request failed: {r.status_code} {r.text[:200]}")

        raise RuntimeError(last_err or "unreachable")


async def fetch_all_nm_ids(client: WBClient, limit: int = 100, max_pages: int = 20) -> list[int]:
    page = 1
    nm_ids: list[int] = []
    prev_page_ids: set[int] = set()

    while page <= max_pages:
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
        products = data.get("products") or []
        if not products:
            break

        page_ids: set[int] = set()
        for p in products:
            nm = p.get("id") or p.get("nmId") or p.get("nm")
            try:
                page_ids.add(int(nm))
            except (TypeError, ValueError):
                pass

        if page_ids and page_ids == prev_page_ids:
            break
        prev_page_ids = page_ids

        nm_ids.extend(page_ids)

        if len(products) < limit:
            break

        page += 1
        await asyncio.sleep(0.35)

    return list(dict.fromkeys(nm_ids))


async def fetch_detail(client: WBClient, nm_id: int) -> dict | None:
    params = {
        "appType": 1,
        "curr": "rub",
        "dest": -1257786,
        "spp": 0,
        "nm": str(nm_id),
        "regions": 86,
    }
    try:
        data = await client.get_json(DETAIL_URL, params=params)
    except Exception as e:
        return None

    products = ((data.get("data") or {}).get("products")) or []
    return products[0] if products else None



def parse_row_and_chars(p: dict) -> tuple[list[Any], dict]:
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

    row = [
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
    return row, characteristics


def make_wb_sheet(title: str) -> tuple[Workbook, Any]:
    wb = Workbook()
    ws = wb.active
    ws.title = title
    ws.append(
        [
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
        ]
    )
    return wb, ws


async def main() -> None:
    wb_full, ws_full = make_wb_sheet("catalog_full")
    wb_filt, ws_filt = make_wb_sheet("catalog_filtered")

    ok_full = 0
    ok_filt = 0
    failed = 0
    empty = 0

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json,text/plain,*/*",
        "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
        "Connection": "keep-alive",
    }

    try:
        async with httpx.AsyncClient(timeout=30.0, headers=headers) as http:
            client = WBClient(http=http, sem=asyncio.Semaphore(2), max_attempts=10)

            nm_ids = await fetch_all_nm_ids(client, limit=100, max_pages=20)
            if not nm_ids:
                return

            batch_size = 100

            for i in range(0, len(nm_ids), batch_size):
                batch = nm_ids[i : i + batch_size]

                tasks = [asyncio.create_task(fetch_detail(client, nm)) for nm in batch]

                for fut in asyncio.as_completed(tasks):
                    try:
                        d = await fut
                    except Exception as e:
                        failed += 1
                        continue

                    if not d:
                        empty += 1
                        continue

                    row, ch = parse_row_and_chars(d)
                    ws_full.append(row)
                    ok_full += 1

                    price = row[3]
                    rating = row[11]

                    if price is None or rating is None:
                        continue

                    try:
                        if float(rating) < 4.5:
                            continue
                        if float(price) > 10000:
                            continue
                    except (TypeError, ValueError):
                        continue

                    if not has_russia(ch):
                        continue

                    ws_filt.append(row)
                    ok_filt += 1

                await asyncio.sleep(0.4)


    finally:
        wb_full.save("wb_catalog_full.xlsx")
        wb_filt.save("wb_catalog_filtered.xlsx")

if __name__ == "__main__":
    asyncio.run(main())
