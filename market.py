import datetime
import logging.config
from environs import Env
from seller import download_stock

import requests

from seller import divide, price_conversion

logger = logging.getLogger(__file__)


def get_product_list(page, campaign_id, access_token):
    """Получить список товаров из каталога магазина.

    Args:
        page (str): Идентификатор страницы c результатами.
            Если параметр не указан, возвращается самая
            старая страница.
        campaign_id (str): Идентификатор кампании.
        access_token (str): Данные авторизации.

    Returns:
        response_object.get("result") (dict): Содержит
            развёрнутую информацию о товарах, содержащихся
            в каталоге магазина.

    .. _YandexMarket API reference:
        https://yandex.ru/dev/market/partner-api/doc/ru/reference/offer-mappings/getOfferMappingEntries

    Raises:
        AttributeError: Если какой-либо из обязательных аргументов отсутствует.
        requests.HTTPError: При возникновении ошибки HTTP.
        requests.ConnectionError: При возникновении ошибки соединения.
        requests.JSONDecodeError: При ошибке во время декодирования в json.

    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {
        "page_token": page,
        "limit": 200,
    }
    url = endpoint_url + f"campaigns/{campaign_id}/offer-mapping-entries"
    response = requests.get(url, headers=headers, params=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def update_stocks(stocks, campaign_id, access_token):
    """Передать информацию по остаткам.

    Передает данные об остатках товаров на витрине. В одном
    запросе можно передать от одного до 2000 товаров.

    Args:
        stocks (list of dict): Информация об остатках
            товара (товаров) на складе.
        campaign_id (int): Идентификатор кампании.
        access_token (str): Данные авторизации.

    Returns:
        response_object (requests.Response) : в случае успеха:
            Body:
                {
                    "status": "OK"
                }

    .. _YandexMarket API reference:
        https://yandex.ru/dev/market/partner-api/doc/ru/reference/stocks/updateStocks

    Raises:
        AttributeError: Если какой-либо из обязательных аргументов отсутствует.
        requests.HTTPError: При возникновении ошибки HTTP.
        requests.ConnectionError: При возникновении ошибки соединения.
        requests.JSONDecodeError: При ошибке во время декодирования в json.

    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"skus": stocks}
    url = endpoint_url + f"campaigns/{campaign_id}/offers/stocks"
    response = requests.put(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def update_price(prices, campaign_id, access_token):
    """Установить цены на товары.

    В течение минуты можно установить цены для 500 товаров.

    Args:
        prices (list of dict): Список с информацией по
            товарам и новыми ценами на них.
        campaign_id (int): Идентификатор кампании.
        access_token (str): Данные авторизации.

    Returns:
        response_object (requests.Response) : в случае успеха:
            Body:
                {
                    "status": "OK"
                }

    .. _YandexMarket API reference:
        https://yandex.ru/dev/market/partner-api/doc/ru/reference/prices/updatePrices

    Raises:
        AttributeError: Если какой-либо из обязательных аргументов отсутствует.
        requests.HTTPError: При возникновении ошибки HTTP.
        requests.ConnectionError: При возникновении ошибки соединения.
        requests.JSONDecodeError: При ошибке во время декодирования в json.

    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"offers": prices}
    url = endpoint_url + f"campaigns/{campaign_id}/offer-prices/updates"
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def get_offer_ids(campaign_id, market_token):
    """Получить артикулы товаров.

    Args:
        campaign_id (int): Идентификатор кампании.
        market_token (str): Данные маркета для авторизации.

    Returns:
        offer_ids (list) : Возвращает список артикулов (SKU)
            товаров магазина.

    Raises:
        AttributeError: Если какой-либо из обязательных аргументов отсутствует.

    """
    page = ""
    product_list = []
    while True:
        some_prod = get_product_list(page, campaign_id, market_token)
        product_list.extend(some_prod.get("offerMappingEntries"))
        page = some_prod.get("paging").get("nextPageToken")
        if not page:
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer").get("shopSku"))
    return offer_ids


def create_stocks(watch_remnants, offer_ids, warehouse_id):
    """Сформировать остатки.

    Args:
        watch_remnants (dict): Словарь с актуальной информацией с сайта
            по остаткам товара.
        offer_ids (list): Список содержащий перечисление артикулов
            всех товаров магазина.
        warehouse_id (int):Идентификатор склада

    Returns:
        В случае успешного выполнения запроса:
            stocks (list of dict): Актуальная информация по остаткам
            товаров. Список словарей, либо пустой список.

    Raises:
        AttributeError: Если какой-либо из обязательных
            аргументов отсутствует.

    """
    # Уберем то, что не загружено в market
    stocks = list()
    date = str(datetime.datetime.utcnow().replace(
        microsecond=0).isoformat() + "Z")
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append(
                {
                    "sku": str(watch.get("Код")),
                    "warehouseId": warehouse_id,
                    "items": [
                        {
                            "count": stock,
                            "type": "FIT",
                            "updatedAt": date,
                        }
                    ],
                }
            )
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append(
            {
                "sku": offer_id,
                "warehouseId": warehouse_id,
                "items": [
                    {
                        "count": 0,
                        "type": "FIT",
                        "updatedAt": date,
                    }
                ],
            }
        )
    return stocks


def create_prices(watch_remnants, offer_ids):
    """Создать список цен.

    Args:
        watch_remnants (dict): Идентификатор клиента.
        offer_ids (list): Список содержащий перечисление
        артикулов всех товаров магазина.

    Returns:
        prices (list of dict): Список содержащий словари
        с указанием стоимости товаров в магазине.

    Raises:
        AttributeError: Если какой-либо из обязательных
            аргументов отсутствует.

    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "id": str(watch.get("Код")),
                # "feed": {"id": 0},
                "price": {
                    "value": int(price_conversion(watch.get("Цена"))),
                    # "discountBase": 0,
                    "currencyId": "RUR",
                    # "vat": 0,
                },
                # "marketSku": 0,
                # "shopSku": "string",
            }
            prices.append(price)
    return prices


async def upload_prices(watch_remnants, campaign_id, market_token):
    """Обновить цены товаров без изменения их карточек в асинхронном режиме.

    Args:
        watch_remnants (dict): Информация с сайта по остаткам товара.
        campaign_id (int): Идентификатор кампании.
        market_token (str): Данные маркета для авторизации.

    Returns:
        prices (list of dict): Список словарей со стоимостью товаров.

    Examples:
        >>> print(isinstance(upload_prices, list)))
        True

    Raises:
        AttributeError: Если какой-либо из обязательных аргументов отсутствует.

    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_prices in list(divide(prices, 500)):
        update_price(some_prices, campaign_id, market_token)
    return prices


async def upload_stocks(
        watch_remnants, campaign_id,
        market_token, warehouse_id,
):
    """Обновить остатки товаров в асинхронном режиме.

    Args:
        watch_remnants (dict): Информация с сайта по остаткам товара.
        campaign_id (int): Идентификатор кампании.
        market_token (str): Данные маркета для авторизации.
        warehouse_id (str):Идентификатор склада

    Returns:
        not_empty (list): Список товаров, запасы которых не равны нулю.
        stocks (list of dict): Актуальная информация по остаткам товаров.

    Examples:
        >>> print(isinstance(upload_stocks, (list, list)))
        True

    Raises:
        AttributeError: Если какой-либо из обязательных
            аргументов отсутствует.

    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    stocks = create_stocks(watch_remnants, offer_ids, warehouse_id)
    for some_stock in list(divide(stocks, 2000)):
        update_stocks(some_stock, campaign_id, market_token)
    not_empty = list(
        filter(lambda stock: (stock.get("items")[0].get("count") != 0), stocks)
    )
    return not_empty, stocks


def main():
    env = Env()
    market_token = env.str("MARKET_TOKEN")
    campaign_fbs_id = env.str("FBS_ID")
    campaign_dbs_id = env.str("DBS_ID")
    warehouse_fbs_id = env.str("WAREHOUSE_FBS_ID")
    warehouse_dbs_id = env.str("WAREHOUSE_DBS_ID")

    watch_remnants = download_stock()
    try:
        # FBS
        offer_ids = get_offer_ids(campaign_fbs_id, market_token)
        # Обновить остатки FBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_fbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_fbs_id, market_token)
        # Поменять цены FBS
        upload_prices(watch_remnants, campaign_fbs_id, market_token)

        # DBS
        offer_ids = get_offer_ids(campaign_dbs_id, market_token)
        # Обновить остатки DBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_dbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_dbs_id, market_token)
        # Поменять цены DBS
        upload_prices(watch_remnants, campaign_dbs_id, market_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
