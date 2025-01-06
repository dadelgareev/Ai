import os
import json
import csv
import logging
import uuid
from collections import Counter
from bs4 import BeautifulSoup
import requests

# Настройка логирования
logging.basicConfig(
    filename='scraper.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class TsumScraper:
    def __init__(self):
        self.list_categories = {
            "man_shoes": ["https://www.tsum.ru/catalog/obuv-18440/", "Man"],
            "man_clothes": ["https://www.tsum.ru/catalog/odezhda-2409/", "Man"],
            "women_shoes": ["https://www.tsum.ru/catalog/obuv-18405/", "Woman"],
            "women_clothes": ["https://www.tsum.ru/catalog/odezhda-18413/", "Woman"]
        }
        self.namespace = uuid.NAMESPACE_DNS
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36'
        }
        self.categories_constants = {
    "Блузы и рубашки": [
        "Блузы",
        "Рубашки",
        "Рубашки с длинным рукавом",
        "Джинсовые рубашки",
        "Рубашки с коротким рукавом",
        "Рубашки и сорочки",
        "Джинсовые",
        "Блузы и рубашки",
        "Блузы с коротким рукавом",
        "Блузы с длинным рукавом",
        "Блузы с рюшами и воланами",
        "Кружевные блузы",
        "Блузы без рукавов",
        "Блузы с бантом",
        "Блузы с открытыми плечами",
        "Рубашки и топы"
    ],
    "Брюки": [
        "Брюки",
        "Бриджи и капри",
        "Горнолыжные брюки",
        "Джоггеры",
        "Карго",
        "Классические брюки",
        "Кожаные брюки",
        "Кюлоты",
        "Леггинсы",
        "Повседневные брюки",
        "Спортивные брюки",
        "Тайтсы",
        "Зауженные брюки",
        "Утепленные брюки",
        "Брюки и шорты",
        "Прямые брюки",
        "Леггинсы и тайтсы",
        "Прямые",
        "Бордшорты"
    ],
    "Верхняя одежда": [
        "Анораки",
        "Бомберы",
        "Горнолыжные куртки",
        "Демисезонные куртки",
        "Джинсовые куртки",
        "Кожаные куртки",
        "Легкие куртки и ветровки",
        "Пальто",
        "Парки",
        "Плащи и тренчи",
        "Пончо и кейпы",
        "Пуховики и зимние куртки",
        "Утепленные костюмы и комбинезоны",
        "Шубы и дубленки",
        "Зимние куртки",
        "Верхняя одежда",
        "Пуховики",
        "Куртки",
        "Плащи",
        "Толстовки и куртки",
        "Куртки меховые",
        "Одежда",
        "Шубы",
        "Демисезонные пальто",
        "Двубортные пальто",
        "Классические",
        "Ветровки и куртки",
        "Плащи и тренчкоты",
        "Тренчи",
        "Спортивные",
        "Дубленки",
        "Широкие и расклешенные",
        "Ветровки",
        "Зимние пальто",
        "Летние пальто",
        "Пальто меховые",
        "Накидки и пончо",
        "Дублёнки",
        "Тренчкоты",
        "Кейпы и накидки"
    ],
    "Джемперы, свитеры, кардиганы": [
        "Водолазки",
        "Джемперы и пуловеры",
        "Жилеты",
        "Кардиганы",
        "Свитеры",
        "Джемперы",
        "Джемперы и кардиганы",
        "Джемперы, свитеры и кардиганы",
        "Джемперы",
        "Утепленные жилеты",
        "Меховые жилеты",
        "Удлиненные жилеты",
        "Утепленные",
        "Кожаные жилеты",
        "Джинсовые жилеты",
        "Пончо, жилеты и накидки",
        "Накидки"
    ],
    "Джинсы": [
        "Джеггинсы",
        "Прямые джинсы",
        "Узкие джинсы",
        "Широкие и расклешенные джинсы",
        "Зауженные джинсы",
        "Зауженные джинсы",
        "Джинсы",
        "Зауженные",
        "Классические",
        "Широкие джинсы",
        "Расклешенные джинсы",
        "Джинсы-бойфренды",
        "Джинсы-мом"
    ],
    "Домашняя одежда": [
        "Комбинезоны",
        "Комплекты",
        "Маски для сна",
        "Ночные сорочки",
        "Пижамы",
        "Халаты",
        "Хлопковые халаты",
        "Махровые халаты",
        "Домашняя одежда",
        "Бархатные халаты",
        "Атласные и кружевные халаты",
        "Пижамы с брюками",
        "Пижамы с шортами",
        "Трикотажные халаты",
        "Флисовые халаты",
        "Сорочки"
    ],
    "Комбинезоны": [
        "Джинсовые комбинезоны",
        "Кигуруми",
        "Комбинезоны с брюками",
        "Комбинезоны с шортами",
        "Спортивные комбинезоны",
        "Вечерние комбинезоны",
        "Комбинезоны джинсовые",
        "Брюки и комбинезоны"
    ],
    "Купальники и пляжная одежда": [
        "Лифы",
        "Парео",
        "Плавки",
        "Пляжные платья и туники",
        "Раздельные купальники",
        "Слитные купальники и монокини",
        "Плавки и шорты для плавания",
        "Шорты для плавания",
        "Слитные купальники",
        "Купальники и пляжная одежда",
        "Купальники-халтер",
        "Плавки с завышенной талией",
        "Купальники и парео",
        "Спортивные купальники",
        "Купальники-бралетт",
        "Плавки-бразилиана",
        "Плавки на завязках",
        "Монокини",
        "Купальники-бандо",
        "Купальники",
        "Пляжная одежда"
    ],
    "Нижнее белье": [
        "Аксессуары",
        "Бюстгальтеры",
        "Комбинации",
        "Комплекты",
        "Корректирующее белье",
        "Корсеты",
        "Пояса для чулок",
        "Термобелье",
        "Трусы",
        "Эротическое белье",
        "Брифы",
        "Трусы-шорты",
        "Боксеры",
        "Бралетт",
        "Нижнее белье",
        "Стринги",
        "Верх",
        "Низ",
        "Бесшовные",
        "Бесшовные трусы",
        "Трусы-бразилиана",
        "Слипы",
        "Корректирующее",
        "Комплекты трусов",
        "Эротическое",
        "Балконет",
        "Лифы-бралетт",
        "С треугольными чашечками",
        "Слитные",
        "Бандо",
        "Лифы-бандо",
        "Лифы-халтер",
        "Бандажи",
        "Комплекты белья"
    ],
    "Носки, чулки, колготки": [
        "Гольфы и гетры",
        "Колготки",
        "Короткие носки",
        "Носки",
        "Подследники",
        "Чулки",
        "Кальсоны",
        "Носки и гетры",
        "Носки и колготки",
        "Гетры",
        "Гетры и колготки",
        "Носки, чулки и колготки"
    ],
    "Пиджаки и костюмы": [
        "Жакеты",
        "Кимоно",
        "Костюмы с брюками",
        "Костюмы с шортами",
        "Костюмы с юбкой",
        "Пиджаки",
        "Костюмы",
        "Классические костюмы",
        "Спортивные костюмы",
        "Пиджаки и костюмы",
        "Жакеты и пиджаки",
        "Костюмы и комбинезоны",
        "Костюмы и жакеты",
        "Вязаные костюмы",
        "Смокинги"
    ],
    "Платья и сарафаны": [
        "Вечерние платья",
        "Джинсовые платья",
        "Кожаные платья",
        "Платья с запахом",
        "Платья со спущенными плечами",
        "Повседневные платья",
        "Сарафаны",
        "Свадебные платья",
        "Платья",
        "Платья-рубашки",
        "Трикотажные платья",
        "Платья-комбинации",
        "Платья-футляр",
        "Платья-пиджаки",
        "Платья-футболки",
        "Платья-майки",
        "Платья и сарафаны",
        "Платья и туники",
        "Кружевные платья"
    ],
    "Топы и майки": [
        "Вязаные топы",
        "Корсеты",
        "Майки",
        "Спортивные майки",
        "Спортивные топы",
        "Топы в бельевом стиле",
        "Топы на бретелях",
        "Топы с баской",
        "Топы свободного кроя",
        "Топы со спущенными плечами",
        "Топы",
        "Топы и майки",
        "Боди с коротким рукавом",
        "Боди",
        "Боди с длинным рукавом",
        "Кроп-топы",
        "Спортивные топы и майки",
        "Майки и топы"
    ],
    "Футболки и поло": [
        "Комплекты",
        "Лонгсливы",
        "Поло",
        "Спортивные футболки и лонгсливы",
        "Футболки",
        "Туники",
        "Футболки и майки",
        "Поло с коротким рукавом",
        "Футболки и поло",
        "Футболки с коротким рукавом",
        "Футболки и лонгсливы",
        "Спортивные футболки",
        "Спортивные лонгсливы",
        "Рашгарды"
    ],
    "Худи и свитшоты": [
        "Олимпийки",
        "Свитшоты",
        "Толстовки",
        "Флиски",
        "Худи",
        "Толстовки и свитшоты",
        "Поло с длинным рукавом",
        "Худи и свитшоты",
        "Олимпийки и толстовки",
        "Платья-толстовки",
        "Пуловеры",
        "Толстовки и олимпийки"
    ],
    "Шорты": [
        "Бермуды",
        "Велосипедки",
        "Джинсовые шорты",
        "Карго",
        "Повседневные шорты",
        "Спортивные шорты",
        "Шорты",
        "Шортики",
        "Кожаные шорты"
    ],
    "Юбки": [
        "Джинсовые юбки",
        "Кожаные юбки",
        "Плиссированные юбки",
        "Прямые юбки",
        "Узкие юбки",
        "Юбки",
        "Юбки-трапеции и широкие",
        "Юбки-шорты",
        "Юбки и парео"
    ],
    "Прочее": [
        "Уход за одеждой",
        "Шнурки",
        "Уход за обувью",
        "Экипировка",
        "Спортивный инвентарь",
        "Аксессуары для путешествий",
        "Аксессуары для обуви",
        "Пляжные принадлежности"
    ],
    "Балетки": [
        "Балетки с квадратным носом",
        "Балетки с круглым носом",
        "Балетки с острым носом",
        "Балетки"
    ],
    "Ботильоны": [
        "Ботильоны с квадратным носом",
        "Ботильоны с круглым носом",
        "Ботильоны с острым носом",
        "Ботильоны с открытым носом",
        "Высокие ботильоны",
        "Низкие ботильоны",
        "Ботильоны",
        "Ботинки и полусапоги"
    ],
    "Ботинки": [
        "Высокие ботинки",
        "Мартинсы и др.",
        "Низкие ботинки",
        "Оксфорды и дерби",
        "Тимберленды и др.",
        "Трекинговые ботинки",
        "Челси",
        "Казаки",
        "Дезерты",
        "Ботинки",
        "Обувь"
    ],
    "Вечерняя обувь": [
        "Свадебные туфли",
        "Туфли с застежкой на лодыжке",
        "Туфли с открытой пяткой",
        "Туфли с открытой стопой",
        "Туфли с открытым носом",
        "Вечерняя обувь"
    ],
    "Домашняя обувь": [
        "Сланцы",
        "Домашняя обувь"
    ],
    "Кроссовки и кеды": [
        "Кеды",
        "Высокие кеды",
        "Низкие кеды",
        "Кроссовки",
        "Высокие кроссовки",
        "Низкие кроссовки",
        "Бутсы"
    ],
    "Мокасины и топсайдеры": [
        "Мокасины",
        "Топсайдеры",
        "Мокасины и топсайдеры"
    ],
    "Обувь с увеличенной полнотой": [
        "Обувь с увеличенной полнотой"
    ],
    "Резиновая обувь": [
        "Галоши",
        "Джиббитсы",
        "Акваобувь",
        "Резиновая обувь",
        "Кроксы и др."
    ],
    "Сабо и мюли": [
        "Сабо и мюли",
        "Сабо",
        "Мюли"
    ],
    "Сандалии": [
        "Эспадрильи",
        "Сланцы",
        "Повседневные сандалии",
        "Спортивные сандалии",
        "Сандалии",
        "Сандалии на завязках"
    ],
    "Сапоги": [
        "Ботфорты",
        "Валенки",
        "Дутики",
        "Полусапоги",
        "Сапоги",
        "Угги и унты",
        "Сапоги-чулки",
        "Сапоги-трубы"
    ],
    "Слипоны": [
        "Высокие слипоны",
        "Низкие слипоны",
        "Слипоны"
    ],
    "Туфли": [
        "Закрытые туфли",
        "Лодочки",
        "Лоферы",
        "Туфли Мэри Джейн",
        "Босоножки",
        "Монки",
        "Оксфорды",
        "Дерби",
        "Туфли",
        "Туфли с открытыми боками",
        "Туфли на шнурках"
    ]
}
    def get_category_for_subcategory(self, subcategory):
        """
        Функция для поиска категории по подкатегории.
        Возвращает категорию (ключ) из словаря, если подкатегория найдена.
        """
        for category, subcategories in self.categories_constants.items():
            if subcategory in subcategories:
                return category
        return "Не указано"  # Если подкатегория не найдена, возвращаем "Не указано"

    def fetch_page(self, custom_url, page_number=0):
        # Определяем URL: либо custom_url, либо формируем стандартный
        if page_number == 0:
            url = custom_url
        else:
            url = f"{custom_url}?page={page_number}"
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            #logging.info(f"Успешный запрос к URL: {url}")
            return response.text
        except requests.RequestException as e:
            logging.error(f"Ошибка при запросе к URL {url}: {e}")
            return None


    def get_full_width_elements(self, url):
        html = self.fetch_page(url)
        if html:
            try:
                soup = BeautifulSoup(html, 'html.parser')
                category_elements = soup.find_all('div', class_='x-tree-view-catalog-navigation__category')

                categories_info = []
                for element in category_elements:
                    if element.get('class') == ['x-tree-view-catalog-navigation__category']:
                        link = element.find('a', class_='x-link')
                        count = element.find('span', class_='x-tree-view-catalog-navigation__found')

                        if link and count:
                            category_name = link.text.strip()
                            category_url = link['href']
                            item_count = count.text.strip()
                            categories_info.append({
                                'category_name': category_name,
                                'category_url': category_url,
                                'item_count': item_count
                            })

                logging.info(f"Найдено {len(categories_info)} элементов на странице {url}")
                return categories_info
            except Exception as e:
                logging.error(f"Ошибка при обработке HTML для URL {url}: {e}")
                return []
        else:
            logging.warning(f"HTML-код для URL {url} пуст")
            return []

    def extract_categories(self, url):
        """
        Извлекает категории и ссылки из HTML-кода.
        """
        html = self.fetch_page(url)  # Получаем HTML-страницу с указанного URL
        if html:
            try:
                soup = BeautifulSoup(html, 'html.parser')

                # Находим все элементы списка категорий
                items = soup.find_all('li', class_='MenuFilters__filtersItem___oY3b3')

                # Извлекаем названия категорий и ссылки
                categories = []
                for item in items:
                    link = item.find('a')  # Находим тег <a> внутри элемента
                    if link:
                        href = link['href']  # Извлекаем ссылку
                        category_name = link.get_text(strip=True)  # Извлекаем текст категории
                        # Добавляем словарь с категорией и ссылкой в список
                        categories.append({"name": category_name, "url": href})

                logging.info(f"Найдено {len(categories)} элементов на странице {url}")
                return categories
            except Exception as e:
                logging.error(f"Ошибка при обработке HTML для URL {url}: {e}")
                return []
        else:
            logging.warning(f"HTML-код для URL {url} пуст")
            return []

    def parse_count_pages(self, url, category):
        """
        Извлекает и возвращает количество страниц из JSON-объекта, содержащегося в HTML.

        Аргументы:
            html (str): HTML-код страницы.
            category (str): Название категории, для которой нужно найти количество страниц.

        Возвращает:
            int: Количество страниц для категории, или 0, если не найдено.
        """
        try:
            html = self.fetch_page(url)
            # Парсим HTML с помощью BeautifulSoup
            soup = BeautifulSoup(html, 'html.parser')

            # Ищем теги <script> с атрибутом data-app="true"
            script_tags = soup.find_all('script', attrs={'data-app': 'true'})
            if not script_tags:
                logging.warning("Теги <script> с атрибутом data-app='true' не найдены.")
                return 0

            # Обрабатываем каждый найденный <script>
            for script in script_tags:
                script_content = script.string
                if script_content and "globalThis.initialState" in script_content:
                    try:
                        # Извлекаем JSON из строки
                        json_start = script_content.find("{")
                        json_data = script_content[json_start:]
                        data_dict = json.loads(json_data)

                        # Достаем pageCount для указанной категории
                        page_count = int(data_dict["catalogs"]["list"][category]["data"]["pageCount"])
                        logging.info(f"Найдено {page_count} страниц для категории '{category}'.")
                        return page_count
                    except KeyError as ke:
                        logging.error(f"Ключ '{ke}' не найден в JSON. Проверьте структуру данных.")
                        return 0
                    except (ValueError, TypeError) as ve:
                        logging.error(f"Ошибка при извлечении pageCount: {ve}")
                        return 0

            # Если ни один JSON не подошел
            logging.warning(f"Подходящий JSON-объект для категории '{category}' не найден.")
            return 0
        except Exception as e:
            logging.error(f"Ошибка при обработке HTML: {e}")
            return 0

    def get_all_atrib_from_page(self, url):
        """
        Извлекает атрибуты продукта со страницы.

        Args:
            url (str): URL страницы продукта.

        Returns:
            dict: Словарь с информацией о продукте (или None в случае ошибки).
        """
        # Получаем HTML страницы
        html = self.fetch_page(url)
        url_tag = url.split('/')[-2]

        # Инициализация переменных для хранения данных
        image_urls = []

        try:
            # Парсим HTML с помощью BeautifulSoup
            soup = BeautifulSoup(html, 'html.parser')

            # Парсим список категорий
            categories_list = [item.get_text(strip=True) for item in soup.find_all('li', class_='Breadcrumbs__item___IVD_E')]

            # Ищем теги <script> с атрибутом data-app="true"
            script_tags = soup.find_all('script', attrs={'data-app': 'true'})
            if not script_tags:
                logging.warning("Теги <script> с атрибутом data-app='true' не найдены.")
                return None

            # Обрабатываем найденные теги <script>
            for script in script_tags:
                script_content = script.string
                if script_content and "globalThis.initialState" in script_content:
                    try:
                        # Извлекаем JSON из строки
                        json_start = script_content.find("{")
                        json_data = script_content[json_start:]
                        data_dict = json.loads(json_data)

                        # Извлечение данных из JSON
                        product_info = data_dict["product"]["product"].get(url_tag, {}).get("product", {})

                        # Извлекаем изображения
                        for image in product_info.get("photos", []):
                            image_urls.append(image.get("large", ""))

                        # Извлекаем теги, бренд, категории, цену
                        tags = product_info.get("information", [])
                        color = product_info.get("color", {}).get("title", "")
                        brand = product_info.get("brand", {}).get("title", "")
                        category = product_info.get("category", {}).get("title", "")
                        price = product_info.get("sizes", [{}])[0].get("price", {}).get("originalPrice", None)

                        tags = self.parse_tags(tags)
                        tags["Цвет"] = color
                        description = product_info.get("description", "Описание не найдено")

                        # Извлекаем артикул
                        article = None
                        for item in tags:
                            if "Артикул:" in item:
                                article = item.split(": ")[-1].strip()
                                break

                        # Формируем результирующий словарь
                        product_data = {
                            "image_urls": image_urls,
                            "tags": tags,
                            #"color": color,
                            "brand": brand,
                            "category": category,
                            "price": price,
                            "article": article,
                            "description": description,
                            "list_categories": categories_list
                        }

                        return product_data

                    except KeyError as ke:
                        logging.error(f"Ключ '{ke}' не найден в JSON. Проверьте структуру данных.")
                        return None
                    except json.JSONDecodeError as je:
                        logging.error(f"Ошибка декодирования JSON: {je}")
                        return None

            logging.warning("Не удалось найти данные о продукте в JSON.")
            return None

        except Exception as e:
            logging.error(f"Произошла ошибка при обработке страницы: {e}")
            return None

    def parse_tags(self, tags):
        result = {}
        for tag in tags:
            if isinstance(tag, dict):  # Если элемент уже словарь
                result.update(tag)  # Добавляем его в результат
            elif isinstance(tag, str) and ':' in tag:  # Если строка и содержит ':'
                key, value = tag.split(':', 1)  # Разделяем по первому двоеточию
                result[key.strip()] = value.strip()  # Убираем лишние пробелы
        return result

    def get_href_list(self, url,page=1, href_list=None):
        """
        Извлекает список уникальных ссылок на продукты с указанной страницы.

        Аргументы:
            page (int): Номер страницы для извлечения ссылок.
            href_list (list): Существующий список ссылок для добавления (по умолчанию пустой список).

        Возвращает:
            list: Обновленный список уникальных ссылок на продукты.
        """


        href_list = href_list or []
        html = self.fetch_page(url, page)
        if html:
            try:
                if not html:
                    logging.warning(f"HTML-код для страницы {page} пуст")
                    return href_list

                # Парсим HTML-код с помощью BeautifulSoup
                soup = BeautifulSoup(html, 'html.parser')

                # Находим все теги <a> с атрибутом href
                links = soup.find_all('a', href=True)

                # Отбираем только ссылки, начинающиеся с '/product/'
                product_links = [link['href'] for link in links if link['href'].startswith('/product/')]

                # Преобразуем относительные ссылки в абсолютные
                base_url = "https://www.tsum.ru"
                full_product_links = [base_url + link for link in product_links]

                # Удаляем дубликаты и добавляем в общий список
                unique_links = set(full_product_links) - set(href_list)
                href_list.extend(unique_links)

                logging.info(f"Добавлено {len(unique_links)} уникальных ссылок с страницы {page}")
                return href_list

            except Exception as e:
                logging.error(f"Ошибка при парсинге ссылок на странице {page}: {e}")
                return href_list

    def find_duplicates(self, all_links):
        duplicates = [link for link, count in Counter(all_links).items() if count > 1]
        logging.info(f"Найдено {len(duplicates)} дубликатов ссылок")
        return duplicates

    def remove_duplicates(self, all_links):
        unique_links = list(set(all_links))
        logging.info(f"Удалено дубликатов, осталось уникальных ссылок: {len(unique_links)}")
        return unique_links

    def get_category_for_subcategory(self, subcategory):
        """
        Функция для поиска категории по подкатегории.
        Возвращает категорию (ключ) из словаря, если подкатегория найдена.
        """
        for category, subcategories in self.categories_constants.items():
            if subcategory in subcategories:
                return category
        return "Не указано"  # Если подкатегория не найдена, возвращаем "Не указано"

    def download_image(self, url, save_dir, image_name):
        try:
            response = requests.get(url)
            if response.status_code == 200:
                image_path = os.path.join(save_dir, image_name)
                with open(image_path, 'wb') as img_file:
                    img_file.write(response.content)
                logging.info(f"Картинка успешно загружена: {image_path}")
                return image_path
            else:
                logging.warning(f"Ошибка при скачивании изображения {url}: статус {response.status_code}")
        except Exception as e:
            logging.error(f"Не удалось скачать изображение {url}: {e}")
        return None

    def update_links_file_json(self, filename, parsed_links):
        current_links = {}
        if os.path.exists(filename):
            with open(filename, "r", encoding="utf-8") as file:
                current_links = json.load(file)

        current_urls = {entry['url'] for entry in current_links.get("links", [])}
        new_links = [link for link in parsed_links if link not in current_urls]

        for link in new_links:
            current_links.setdefault("links", []).append({"url": link, "processed": False})

        with open(filename, "w", encoding="utf-8") as file:
            json.dump(current_links, file, ensure_ascii=False, indent=4)

        logging.info(f"Добавлено {len(new_links)} новых ссылок в {filename}")
        return new_links

    def create_and_append_csv_json(self, json_file, output_csv, main_category, grpc_client=None):
        """
        Добавляет ссылки из JSON в существующий и новый CSV-файлы с категориями, тегами, эмбеддингом и источником.
        Обрабатывает только ссылки со статусом "processed: False". После обработки меняет статус на "processed: True".
        """

        # Создаём имя нового CSV-файла с припиской '_temp'
        temp_output_csv = os.path.splitext(output_csv)[0] + '_temp.csv'

        # Поля CSV-файла
        fieldnames = ['Source', 'Source_csv', 'Guid', 'Image_url', 'Main_photo', 'Guid_list', 'Article', 'Gender',
                      'Category', 'Subcategory', 'Embedding', 'Price', 'Brand', 'Tags']

        # Определяем директорию для изображений
        if main_category[1] == "Man":
            images_dir = os.path.join("Photos", "Man", os.path.splitext(output_csv)[0])
        elif main_category[1] == "Woman":
            images_dir = os.path.join("Photos", "Woman", os.path.splitext(output_csv)[0])
        else:
            images_dir = os.path.splitext(output_csv)[0]  # Для других категорий

        # Создание директории для изображений, если ещё не существует
        if not os.path.exists(images_dir):
            os.makedirs(images_dir)

        # Загрузка ссылок из JSON
        links_data = {}
        if os.path.exists(json_file):
            with open(json_file, 'r', encoding='utf-8') as file:
                links_data = json.load(file)

        # Выбираем только ссылки с "processed: False"
        unprocessed_links = [entry for entry in links_data.get("links", []) if not entry["processed"]]

        # Открываем исходный и новый CSV файлы
        with open(output_csv, 'a', newline='', encoding='utf-8') as old_csvfile, \
                open(temp_output_csv, 'w', newline='', encoding='utf-8') as new_csvfile:

            # Создаем писателей для обоих файлов
            old_writer = csv.DictWriter(old_csvfile, fieldnames=fieldnames)
            new_writer = csv.DictWriter(new_csvfile, fieldnames=fieldnames)



            if old_csvfile.tell() == 0:  # tell возвращает 0, если файл пуст
                old_writer.writeheader()
            new_writer.writeheader()  # Записываем заголовок в новый файл

            # Проходимся по каждой новой ссылке
            for link_entry in unprocessed_links:
                url = link_entry["url"].strip()

                try:
                    # Получаем атрибуты и изображения для текущей ссылки
                    result = self.get_all_atrib_from_page(url)
                except Exception as e:
                    logging.error(f"Ошибка при обработке URL {url}: {e}")
                    continue

                # Берём список URL картинок
                image_urls = result.get('image_urls', [])
                subcategory = result.get('category', "Категория не найдена")

                # Записываем данные для каждой картинки
                guid_list = []
                embedding_list = []
                for index, image_url in enumerate(image_urls):
                    image_name = image_url.split('/')[-1]
                    image_path = os.path.join(images_dir, image_name)
                    try:
                        if not os.path.exists(image_path):
                            self.download_image(image_url, images_dir, image_name)
                    except Exception as e:
                        logging.error(f"Ошибка при скачивании картинки {image_url}: {e}")
                        continue

                    try:
                        with open(image_path, "rb") as image_file:
                            image_data = image_file.read()
                            embedding_norm = grpc_client.get_embedding(image_name=image_path, image_data=image_data)
                            embedding_list.append(embedding_norm)
                    except Exception as e:
                        logging.error(f"Ошибка при получении эмбеддинга через gRPC для {image_name}: {e}")
                        continue

                    # Генерация GUID
                    guid = str(uuid.uuid3(self.namespace, image_url))
                    guid_list.append(guid)

                # Пропускаем ссылку, если количество элементов в списках не совпадает
                if len(guid_list) != len(embedding_list) or len(image_urls) != len(guid_list):
                    logging.warning(f"Пропускаем ссылку {url}, так как не все фотографии были обработаны. Длина guid = {len(guid_list)}, embedding_list = {len(embedding_list)}, image_urls = {len(image_urls)}")
                    continue

                # Обработка изображений с учетом главного фото
                main_photo_guid = guid_list[0] if guid_list else None
                for index, image_url in enumerate(image_urls):
                    row_data = {
                        'Source': 'Lamoda',
                        'Source_csv': output_csv,
                        'Guid': guid_list[index],
                        'Image_url': image_url,
                        'Main_photo': "None" if index == 0 else main_photo_guid,
                        'Guid_list': json.dumps(guid_list, ensure_ascii=False),
                        'Article': result.get('article', "Артикул не найден"),
                        'Gender': main_category[1],
                        'Category': self.get_category_for_subcategory(subcategory),
                        'Subcategory': subcategory,
                        'Embedding': embedding_list[index] if index < len(embedding_list) else "Ошибка",
                        'Price': result.get('price', 'Цена не найдена'),
                        'Brand': result.get('brand', 'Бренд не найден'),
                        'Tags': json.dumps(result.get('tags', []), ensure_ascii=False)
                    }
                    old_writer.writerow(row_data)
                    new_writer.writerow(row_data)

                link_entry["processed"] = True

        # Сохраняем обновлённый JSON с обновлёнными статусами
        with open(json_file, 'w', encoding='utf-8') as file:
            json.dump(links_data, file, ensure_ascii=False, indent=4)
        logging.info(f"Данные добавлены в '{output_csv}' и '{temp_output_csv}'")
        print(f"Данные добавлены в '{output_csv}' и '{temp_output_csv}'")

    def create_and_append_csv_json_fine_tuning(self, json_file, output_csv, main_category, grpc_client=None):
        """
        Добавляет ссылки из JSON в существующий и новый CSV-файлы с категориями, тегами, эмбеддингом и источником.
        Обрабатывает только ссылки со статусом "processed: False". После обработки меняет статус на "processed: True".
        """
        temp_output_csv = os.path.splitext(output_csv)[0] + '_temp.csv'

        # Поля CSV-файла
        fieldnames = ["image_path","list_categories","all_atributes","source","description"]

        # Определяем директорию для изображений
        if main_category[1] == "Man":
            images_dir = os.path.join("Tsum_Photos", "Man", os.path.splitext(output_csv)[0])
        elif main_category[1] == "Woman":
            images_dir = os.path.join("Tsum_Photos", "Woman", os.path.splitext(output_csv)[0])
        else:
            images_dir = os.path.splitext(output_csv)[0]  # Для других категорий

        if not os.path.exists(images_dir):
            os.makedirs(images_dir)

        # Загрузка ссылок из JSON
        links_data = {}
        if os.path.exists(json_file):
            with open(json_file, 'r', encoding='utf-8') as file:
                links_data = json.load(file)

        unprocessed_links = [entry for entry in links_data.get("links", []) if not entry["processed"]]

        # Открываем исходный и новый CSV файлы
        with open(output_csv, 'a', newline='', encoding='utf-8') as old_csvfile, \
                open(temp_output_csv, 'w', newline='', encoding='utf-8') as new_csvfile:

            old_writer = csv.DictWriter(old_csvfile, fieldnames=fieldnames)
            new_writer = csv.DictWriter(new_csvfile, fieldnames=fieldnames)

            if old_csvfile.tell() == 0:
                old_writer.writeheader()
            new_writer.writeheader()

            for link_entry in unprocessed_links:
                url = link_entry["url"].strip()

                try:
                    result = self.get_all_atrib_from_page(url)
                except Exception as e:
                    logging.error(f"Ошибка при обработке URL {url}: {e}")
                    continue

                image_urls = result.get('image_urls', [])
                tags = result.get('tags', {})
                brand = result.get('brand', "Бренд не найден")

                tags["Бренд"] = brand

                all_categories = result.get('list_categories', [])
                if all_categories:
                    all_categories = all_categories[2:]
                description = result.get('description', 'Описание не найдено')

                for index, image_url in enumerate(image_urls):
                    image_name = image_url.split('/')[-1]
                    image_path = os.path.join(images_dir, image_name)
                    try:
                        if not os.path.exists(image_path):
                            self.download_image(image_url, images_dir, image_name)
                    except Exception as e:
                        logging.error(f"Ошибка при скачивании картинки {image_url}: {e}")
                        continue

                for index, image_url in enumerate(image_urls):
                    row_data = {
                        "image_path" : os.path.join(os.getcwd(),os.path.join(images_dir,(image_url.split('/')[-1]))),
                        "list_categories" : all_categories,
                        "all_atributes" : tags,
                        "source" : "Tsum",
                        "description" : description,
                    }
                    old_writer.writerow(row_data)
                    new_writer.writerow(row_data)

                link_entry["processed"] = True

        with open(json_file, 'w', encoding='utf-8') as file:
            json.dump(links_data, file, ensure_ascii=False, indent=4)
        logging.info(f"Данные добавлены в '{output_csv}' и '{temp_output_csv}'")
        print(f"Данные добавлены в '{output_csv}' и '{temp_output_csv}'")

if __name__ == "__main__":
    scraper = TsumScraper()
    main_category = scraper.list_categories["man_clothes"]
    #print(scraper.fetch_page(main_category[0]))

    dict_info = scraper.get_all_atrib_from_page("https://www.tsum.ru/product/7015783-kashemirovoe-palto-must-chernyi/")
    href_list = ["https://www.tsum.ru/product/7015783-kashemirovoe-palto-must-chernyi/"]
    scraper.update_links_file_json("123.json", href_list)
    scraper.create_and_append_csv_json_fine_tuning("123.json", "123.csv",scraper.list_categories["man_clothes"])
    print(dict_info)
    """
    for main_category in scraper.list_categories:
        info = scraper.extract_categories(scraper.list_categories[main_category][0])
        for subcategory_dict in info:
            href_list = []
            subcategory_short_url = subcategory_dict["url"].split('/')[-2]
            print(subcategory_short_url)
            scraper.base_url = "https://www.tsum.ru" + subcategory_dict["url"]
            print(subcategory_dict)
            href_list = scraper.get_href_list(1, href_list)
            print(href_list)
            print(main_category)
            #scraper.update_links_file_json(f"Tsum-{scraper.list_categories[main_category][1]}-{subcategory_short_url}.json",href_list)
            #scraper.create_and_append_csv_json(f"Tsum-{scraper.list_categories[main_category][1]}-{subcategory_short_url}.json",
                                               #f"Tsum-{scraper.list_categories[main_category][1]}-{subcategory_short_url}.csv",
                                               #scraper.list_categories[main_category])
            break
        break
    """