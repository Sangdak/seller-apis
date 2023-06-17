import io
import logging.config
import os
import re
import zipfile
from environs import Env

import pandas as pd
import requests

logger = logging.getLogger(__file__)


def get_product_list(last_id, client_id, seller_token):
    """Получить список товаров магазина.

    Метод позволяет использовать фильтры, чтобы разбить товары на группы
    по статусу видимости или отслеживать изменение их статуса с помощью
    идентификатора товара.

    Args:
        last_id (str): Идентификатор последнего значения на странице.
            Оставьте это поле пустым при выполнении первого запроса. Чтобы
            получить следующие значения, укажите `last_id` из ответа
            предыдущего запроса.
        client_id (str): Идентификатор клиента.
        seller_token (str): API-ключ.

    Returns:
        json: В случае успешного выполнения запроса ключи:
            - items (list of dicts): Список товаров. Каждый словарь содержит
                ключи `product_id` (int) и `offer_id` (str), необходимые для
                идентификации товара. Требуются для выполнения последующих
                запросов.
            - last_id (str): Идентификатор последнего значения на странице.
            - total (int): Общее количество товаров.

        json: В случае неудачного выполнения запроса ключи:
            - code (int): Код ошибки.
            - details (list of dicts): Дополнительная информация об ошибке.
                Каждый словарь содержит ключи `typeUrl` (str) и `value` (str).
            - message (str): Описание ошибки.

    Raises:
        AttributeError: Если какой-либо из обязательных аргументов отсутствует.

    """
    url = "https://api-seller.ozon.ru/v2/product/list"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {
        "filter": {
            "visibility": "ALL",
        },
        "last_id": last_id,
        "limit": 1000,  # Количество значений. Минимум — 1, максимум — 1000.
    }
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def get_offer_ids(client_id, seller_token):
    """Получить артикулы товаров в магазине.

    Посредством обращения к API магазина формируется список товаров
    `product_list`, содержащий идентификаторы `product_id` и
    артикулы `offer_id` каждого товара. В дальнейшем формируется
    список, содержащий только артикулы товаров.

    Args:
        client_id (str): Идентификатор клиента.
        seller_token (str): API-ключ.

    Returns:
        offer_ids (list): Список содержащий перечисление артикулов
            всех товаров магазина.

    Examples:
        >>> print(isinstance(offer_ids, list)))
        True

    Raises:
        AttributeError: Если какой-либо из обязательных аргументов отсутствует.

    """
    last_id = ""
    product_list = []
    while True:
        some_prod = get_product_list(last_id, client_id, seller_token)
        product_list.extend(some_prod.get("items"))
        total = some_prod.get("total")
        last_id = some_prod.get("last_id")
        if total == len(product_list):
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer_id"))
    return offer_ids


def update_price(prices: list, client_id, seller_token):
    """Обновить цены товаров.

    Позволяет изменить цену одного или нескольких товаров не меняя их карточек.
    За один запрос можно изменить цены для 1000 товаров. Новая цена должна
    отличаться от старой минимум на 5%.

    Args:
        prices (list of dicts): Информация о ценах товаров.

            [
                {
                    "auto_action_enabled": "UNKNOWN",
                    "currency_code": "RUB",
                    "min_price": "800",
                    "offer_id": "",
                    "old_price": "0",
                    "price": "1448",
                    "product_id": 1386
                }
                . . .
            ]

        client_id (str): Идентификатор клиента.
        seller_token (str): API-ключ.

    Returns:
        В случае успешного выполнения запроса:
            Массив json вида:

                {
                    "result": [
                        {
                            "product_id": 1386,  # Идентификатор товара.
                            "offer_id": "PH8865",  # Артикул.
                            "updated": true,  # Если обновлено — `true`.
                            "errors": [ ]  # Массив ошибок, если  есть.
                        }
                    ]
                }

        В случае неудачного выполнения запроса:
            см. аналогичный ответ в объявлении функции `get_product_list`.

    Raises:
        AttributeError: Если какой-либо из обязательных аргументов отсутствует.
        requests.HTTPError: При возникновении ошибки HTTP.
        requests.ConnectionError: При возникновении ошибки соединения.
        requests.JSONDecodeError: При ошибке во время декодирования в json.

    """
    url = "https://api-seller.ozon.ru/v1/product/import/prices"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"prices": prices}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def update_stocks(stocks: list, client_id, seller_token):
    """Обновить остатки товаров.

    Позволяет менять информацию о количестве товара в наличии.
    Метод используется только для FBS и rFBS складов. За один
    запрос можно изменить наличие для 100 товаров. В минуту
    можно отправить до 80 запросов.

    Args:
        stocks: Массив json -  Информация о товарах на складах.

            {
                "stocks": [
                    {
                        "offer_id": "PG-2404С1",
                        "product_id": 55946,
                        "stock": 4
                    }
                    . . .
                ]
            }

        client_id (str): Идентификатор клиента.
        seller_token (str): API-ключ.

    Returns:
        В случае успешного выполнения запроса:
            Массив json вида:

                {
                    "result": [
                        {
                            "product_id": 1386,
                            "offer_id": "PH8865",
                            "updated": true,
                            "errors": [ ]
                        }
                    ]
                }

        В случае неудачного выполнения запроса:
            см. аналогичный ответ в объявлении функции `get_product_list`.

    Raises:
        AttributeError: Если какой-либо из обязательных аргументов отсутствует.
        requests.HTTPError: При возникновении ошибки HTTP.
        requests.ConnectionError: При возникновении ошибки соединения.
        requests.JSONDecodeError: При ошибке во время декодирования в json.

    """
    url = "https://api-seller.ozon.ru/v1/product/import/stocks"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"stocks": stocks}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def download_stock():
    """Скачать файл `ostatki` с сайта casio.

    Скачивает с сайта *.zip/*.xls файл с информацией об остатках товара
    и переводит содержащуюся в нём информацию в тип словаря.

    Returns:
        watch_remnants (dict): Информация с сайта по остаткам товара.

    Examples:
        >>> print(isinstance(download_stock, dict)))
        True

    Raises:
        TypeError: Если файл `ostatki` не является текстовым файлом *.xls.
        requests.HTTPError: При возникновении ошибки HTTP.
        requests.ConnectionError: При возникновении ошибки соединения.

    """
    # Скачать остатки с сайта
    casio_url = "https://timeworld.ru/upload/files/ostatki.zip"
    session = requests.Session()
    response = session.get(casio_url)
    response.raise_for_status()
    with response, zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        archive.extractall(".")
    # Создаем список остатков часов:
    excel_file = "ostatki.xls"
    watch_remnants = pd.read_excel(
        io=excel_file,
        na_values=None,
        keep_default_na=False,
        header=17,
    ).to_dict(orient="records")
    os.remove("./ostatki.xls")  # Удалить файл
    return watch_remnants


def create_stocks(watch_remnants, offer_ids):
    """Сформировать остатки.

    Args:
        watch_remnants (dict): Словарь с актуальной информацией с сайта
            по остаткам товара.
        offer_ids (list): Список содержащий перечисление артикулов
            всех товаров магазина.

    Returns:
        В случае успешного выполнения запроса:
            stocks (list of dict): Актуальная информация по остаткам товаров.
            Список словарей, либо пустой список. Каждый словарь содержит ключи:
                offer_id (str): Артикул товара.
                stock (int): Количество оставшихся единиц товара.

    Examples:
        >>> print(isinstance(create_stocks, list)))
        True

    Raises:
        AttributeError: Если какой-либо из обязательных аргументов отсутствует.

    """
    # Уберем то, что не загружено в seller
    stocks = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append({"offer_id": str(watch.get("Код")), "stock": stock})
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append({"offer_id": offer_id, "stock": 0})
    return stocks


def create_prices(watch_remnants, offer_ids):
    """Создать список цен.

    Args:
        watch_remnants (dict): Идентификатор клиента.
        offer_ids (list): Список содержащий перечисление артикулов
            всех товаров магазина.

    Returns:
        prices (list of dict): Список содержащий словари с указанием стоимости
            товаров в магазине.

    Examples:
        >>> print(isinstance(offer_ids, list)))
        True

    Raises:
        AttributeError: Если какой-либо из обязательных аргументов отсутствует.

    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": str(watch.get("Код")),
                "old_price": "0",
                "price": price_conversion(watch.get("Цена")),
            }
            prices.append(price)
    return prices


def price_conversion(price: str) -> str:
    """Преобразовать цену.

    Преобразует полученную строку путём выделения целой части цены и удаления
    из неё всех знаков не являющихся арабскими цифрами.

    Args:
        price: Необработанная строка, содержащая цену с лишними символами,
            например, дробной частью и указанием валюты.

    Returns:
        Строка, содержащая целое цифровое значение цены.

    Examples:
        >>> print(price_conversion("5'990.00 руб."))
        5990

    Raises:
        AttributeError:Если тип `price` не является 'str'.

    """
    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int):
    """Разделить список lst на части по n элементов."""
    for i in range(0, len(lst), n):
        yield lst[i: i + n]


async def upload_prices(watch_remnants, client_id, seller_token):
    """Обновить цены товаров без изменения их карточек в асинхронном режиме.

    Args:
        watch_remnants (dict): Информация с сайта по остаткам товара.
        client_id (str): Идентификатор клиента.
        seller_token (str): API-ключ.

    Returns:
        prices (list of dict): Список словарей со стоимостью товаров.

    Examples:
        >>> print(isinstance(upload_prices, list)))
        True

    Raises:
        AttributeError: Если какой-либо из обязательных аргументов отсутствует.

    """
    offer_ids = get_offer_ids(client_id, seller_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_price in list(divide(prices, 1000)):
        update_price(some_price, client_id, seller_token)
    return prices


async def upload_stocks(watch_remnants, client_id, seller_token):
    """Обновить остатки товаров в асинхронном режиме.

    Args:
        watch_remnants (dict): Информация с сайта по остаткам товара.
        client_id (str): Идентификатор клиента.
        seller_token (str): API-ключ.

    Returns:
        not_empty (list): Список товаров, запасы которых не равня нулю.
        stocks (list of dict): Актуальная информация по остаткам товаров.

    Examples:
        >>> print(isinstance(upload_stocks, (list, list)))
        True

    Raises:
        AttributeError: Если какой-либо из обязательных аргументов отсутствует.

    """
    offer_ids = get_offer_ids(client_id, seller_token)
    stocks = create_stocks(watch_remnants, offer_ids)
    for some_stock in list(divide(stocks, 100)):
        update_stocks(some_stock, client_id, seller_token)
    not_empty = list(filter(lambda stock: (stock.get("stock") != 0), stocks))
    return not_empty, stocks


def main():
    env = Env()
    seller_token = env.str("SELLER_TOKEN")
    client_id = env.str("CLIENT_ID")
    try:
        offer_ids = get_offer_ids(client_id, seller_token)
        watch_remnants = download_stock()
        # Обновить остатки
        stocks = create_stocks(watch_remnants, offer_ids)
        for some_stock in list(divide(stocks, 100)):
            update_stocks(some_stock, client_id, seller_token)
        # Поменять цены
        prices = create_prices(watch_remnants, offer_ids)
        for some_price in list(divide(prices, 900)):
            update_price(some_price, client_id, seller_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
