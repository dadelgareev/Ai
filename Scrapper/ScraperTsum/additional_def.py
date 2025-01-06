import json

import requests
from bs4 import BeautifulSoup

categories_constants = {
    "Блузы и рубашки": ["Блузы", "Рубашки", "Боди"],
    "Брюки": [
        "Брюки", "Бриджи и капри", "Горнолыжные брюки", "Джоггеры", "Карго",
        "Классические брюки", "Кожаные брюки", "Кюлоты", "Леггинсы",
        "Повседневные брюки", "Спортивные брюки", "Тайтсы"
    ],
    "Верхняя одежда": [
        "Анораки", "Бомберы", "Горнолыжные куртки", "Демисезонные куртки",
        "Джинсовые куртки", "Жилеты", "Кожаные куртки", "Легкие куртки и ветровки",
        "Пальто", "Парки", "Плащи и тренчи", "Пончо и кейпы", "Пуховики и зимние куртки",
        "Утепленные костюмы и комбинезоны", "Шубы и дубленки"
    ],
    "Джемперы, свитеры, кардиганы": [
        "Водолазки", "Джемперы и пуловеры", "Жилеты", "Кардиганы", "Свитеры"
    ],
    "Джинсы": [
        "Джеггинсы", "Прямые джинсы", "Узкие джинсы", "Широкие и расклешенные джинсы"
    ],
    "Домашняя одежда": [
        "Брюки и шорты", "Джемперы и кардиганы", "Комбинезоны", "Комплекты",
        "Маски для сна", "Ночные сорочки", "Пижамы", "Платья", "Рубашки",
        "Толстовки и свитшоты", "Топы и майки", "Футболки и лонгсливы", "Халаты"
    ],
    "Комбинезоны": [
        "Джинсовые комбинезоны", "Кигуруми", "Комбинезоны с брюками",
        "Комбинезоны с шортами", "Спортивные комбинезоны"
    ],
    "Купальники и пляжная одежда": [
        "Лифы", "Парео", "Плавки", "Пляжные платья и туники",
        "Раздельные купальники", "Слитные купальники и монокини"
    ],
    "Нижнее белье": [
        "Аксессуары", "Боди", "Бюстгальтеры", "Комбинации", "Комплекты",
        "Корректирующее белье", "Корсеты", "Пояса для чулок", "Термобелье",
        "Трусы", "Эротическое белье"
    ],
    "Носки, чулки, колготки": [
        "Гольфы и гетры", "Колготки", "Короткие носки", "Носки",
        "Подследники", "Чулки"
    ],
    "Пиджаки и костюмы": [
        "Жакеты", "Жилеты", "Кимоно", "Костюмы с брюками",
        "Костюмы с шортами", "Костюмы с юбкой", "Пиджаки"
    ],
    "Платья и сарафаны": [
        "Вечерние платья", "Джинсовые платья", "Кожаные платья",
        "Платья с запахом", "Платья со спущенными плечами", "Повседневные платья",
        "Сарафаны", "Свадебные платья"
    ],
    "Топы и майки": [
        "Вязаные топы", "Корсеты", "Майки", "Спортивные майки",
        "Спортивные топы", "Топы в бельевом стиле", "Топы на бретелях",
        "Топы с баской", "Топы свободного кроя", "Топы со спущенными плечами"
    ],
    "Футболки и поло": [
        "Комплекты", "Лонгсливы", "Поло", "Спортивные футболки и лонгсливы", "Футболки", "Туники"
    ],
    "Худи и свитшоты": [
        "Олимпийки", "Свитшоты", "Толстовки", "Флиски", "Худи"
    ],
    "Шорты": [
        "Бермуды", "Велосипедки", "Джинсовые шорты", "Карго",
        "Повседневные шорты", "Спортивные шорты"
    ],
    "Юбки": [
        "Джинсовые юбки", "Кожаные юбки", "Плиссированные юбки",
        "Прямые юбки", "Узкие юбки"
    ],
    "Прочее": ["Уход за одеждой", "Шнурки"],
    "Балетки": [
        "Балетки с квадратным носом", "Балетки с круглым носом", "Балетки с острым носом"
    ],
    "Ботильоны": [
        "Ботильоны с квадратным носом", "Ботильоны с круглым носом",
        "Ботильоны с острым носом", "Ботильоны с открытым носом",
        "Высокие ботильоны", "Низкие ботильоны"
    ],
    "Ботинки": [
        "Высокие ботинки", "Мартинсы и др.", "Низкие ботинки",
        "Оксфорды и дерби", "Тимберленды и др.", "Трекинговые ботинки", "Челси",
        "Казаки", "Дезерты"
    ],
    "Вечерняя обувь": [
        "Свадебные туфли", "Туфли с застежкой на лодыжке",
        "Туфли с открытой пяткой", "Туфли с открытой стопой", "Туфли с открытым носом"
    ],
    "Домашняя обувь": ["Сланцы"],
    "Кроссовки и кеды": [
        "Кеды", "Высокие кеды", "Низкие кеды",
        "Кроссовки", "Высокие кроссовки", "Низкие кроссовки", "Бутсы"
    ],
    "Мокасины и топсайдеры": ["Мокасины", "Топсайдеры"],
    "Обувь с увеличенной полнотой": ["Обувь с увеличенной полнотой"],
    "Резиновая обувь": ["Галоши", "Джиббитсы", "Акваобувь"],
    "Сабо и мюли": ["Сабо и мюли", "Сабо", "Мюли"],
    "Сандалии": ["Эспадрильи", "Сланцы"],
    "Сапоги": [
        "Ботфорты", "Валенки", "Дутики", "Полусапоги", "Сапоги",
        "Угги и унты"
    ],
    "Слипоны": ["Высокие слипоны", "Низкие слипоны"],
    "Туфли": [
        "Закрытые туфли", "Лодочки", "Лоферы", "Туфли Мэри Джейн",
        "Босоножки", "Монки", "Оксфорды", "Дерби"
    ]
}
headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36'
        }

def count_and_extract_text_by_class(html, target_class):
    # Парсим HTML с помощью BeautifulSoup
    soup = BeautifulSoup(html, 'html.parser')

    # Находим все элементы с указанным классом
    elements_with_class = soup.find_all(class_=target_class)

    # Подсчитываем количество элементов
    count = len(elements_with_class)

    # Извлекаем текстовое содержимое элементов
    texts = [element.get_text(strip=True) for element in elements_with_class]

    # Вывод результата
    print(f"Количество элементов с классом '{target_class}': {count}")
    print("Тексты этих элементов:")
    for text in texts:
        print(f"- {text}")

    return count, texts


def get_category_for_subcategory(subcategory):
    """
    Функция для поиска категории по подкатегории.
    Возвращает категорию (ключ) из словаря, если подкатегория найдена.
    """
    for category, subcategories in categories_constants.items():
        if subcategory in subcategories:
            return category
    return "Не указано"  # Если подкатегория не найдена, возвращаем "Не указано"

def save_html_from_url(url, output_file):
    """
    Получает HTML с указанного URL и сохраняет его в локальный файл.

    :param url: str - URL сайта, с которого нужно получить HTML
    :param output_file: str - имя файла, в который сохранить HTML
    """
    try:
        # Отправляем GET-запрос на сайт
        response = requests.get(url, headers = headers)
        response.raise_for_status()  # Проверяем успешность запроса

        # Получаем HTML-контент
        html_content = response.text

        # Сохраняем контент в файл
        with open(output_file, 'w', encoding='utf-8') as file:
            file.write(html_content)

        print(f"HTML успешно сохранён в файл: {output_file}")
    except requests.exceptions.RequestException as e:
        print(f"Ошибка при загрузке HTML: {e}")

# Пример использования функции
subcategory = "Блузы"  # пример подкатегории
category = get_category_for_subcategory(subcategory)

print(category)
def get_values_from_html_class(html_content):
    # Создаем объект BeautifulSoup
    soup = BeautifulSoup(html_content, 'html.parser')

    # Находим все элементы с классом "MenuFilters__filtersItem___oY3b3"
    items = soup.find_all('li', class_='MenuFilters__filtersItem___oY3b3')

    # Извлекаем названия категорий и ссылки
    categories = []
    for item in items:
        link = item.find('a')  # Находим тег <a> внутри элемента
        if link:
            href = link['href']  # Извлекаем ссылку
            category_name = link.get_text(strip=True)  # Извлекаем текст категории
            categories.append({'category': category_name, 'url': href})

    # Печатаем результат
    for category in categories:
        print(f"Категория: {category['category']}, Ссылка: {category['url']}")


def extract_json_from_html(html):
    """
    Извлекает JSON-объект из HTML-страницы, если он содержится в <script> тегах.
    """
    try:
        # Парсим HTML
        soup = BeautifulSoup(html, 'html.parser')

        # Ищем теги <script> с данными
        script_tags = soup.find_all('script', attrs={'data-app': 'true'})

        # Проходимся по найденным тегам
        for script in script_tags:
            # Получаем содержимое тега
            script_content = script.string

            # Проверяем и парсим JSON-объект
            if script_content and "globalThis.initialState" in script_content:
                # Извлекаем JSON из строки
                json_start = script_content.find("{")
                json_data = script_content[json_start:]

                # Конвертируем в словарь
                data_dict = json.loads(json_data)
                return data_dict

        # Если не найден подходящий JSON
        return None
    except Exception as e:
        print(f"Ошибка при извлечении JSON: {e}")
        return None


def find_key_path(data, target_key, current_path=None):
    """
    Рекурсивно ищет путь к ключу в многовложенной структуре (словарь/список).

    :param data: Словарь или список для поиска.
    :param target_key: Ключ, который нужно найти.
    :param current_path: Текущий путь (для рекурсии, начальное значение - пустой список).
    :return: Путь до ключа в виде списка или None, если ключ не найден.
    """
    if current_path is None:
        current_path = []

    if isinstance(data, dict):  # Если элемент - это словарь
        for key, value in data.items():
            new_path = current_path + [key]
            if key == target_key:  # Если нашли ключ
                return new_path
            found_path = find_key_path(value, target_key, new_path)
            if found_path:  # Если нашли в дочернем элементе
                return found_path
    elif isinstance(data, list):  # Если элемент - это список
        for index, item in enumerate(data):
            new_path = current_path + [index]
            found_path = find_key_path(item, target_key, new_path)
            if found_path:  # Если нашли в дочернем элементе
                return found_path

    return None  # Если не нашли ключ в данной ветке

def extract_product_links(html):

    try:
        # Парсим HTML-код с помощью BeautifulSoup
        soup = BeautifulSoup(html, 'html.parser')

        # Находим все теги <a> с атрибутом href
        links = soup.find_all('a', href=True)

        # Отбираем только ссылки, начинающиеся с '/product/'
        product_links = [link['href'] for link in links if link['href'].startswith('/product/')]
        product_links = set(product_links)
        product_links = list(product_links)
        return product_links
    except Exception as e:
        print(f"Ошибка при извлечении ссылок: {e}")
        return []

def search(search_key, search_tree, parent_key=None):
    if search_key in search_tree:
        yield f'{parent_key if parent_key else ""}["{search_key}"]', search_tree[search_key]
    else:
        for sub_key, sub_tree in search_tree.items():
            if isinstance(sub_tree, dict):
                yield from search(search_key, sub_tree, f'{parent_key if parent_key else ""}["{sub_key}"]')

response = requests.get("https://www.tsum.ru/catalog/odezhda-2409/", headers = headers)
response.raise_for_status()  # Проверяем успешность запроса
"""
# Получаем HTML-контент
html_content = response.text
get_values_from_html_class(html_content)
response2 = requests.get("https://www.tsum.ru/catalog/men-kurtki-19386/", headers = headers)
response2.raise_for_status()

html_content = response2.text

slovar = extract_json_from_html(html_content)
#print(slovar)
first_level_keys = list(slovar["catalogs"]["list"]['men-kurtki-19386']["data"].keys())
print(first_level_keys)
#print(slovar['catalogs'])

for key in slovar["catalogs"]["list"]['men-kurtki-19386']["data"].keys():
    print(slovar["catalogs"]["list"]['men-kurtki-19386']["data"][key])
    print(f"{key}////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////")
print(slovar["catalogs"]["list"]['men-kurtki-19386']["data"]["pageCount"])


#save_html_from_url("https://www.tsum.ru/catalog/men-kurtki-19386/", "try_find_count_pages.html")
print(len(extract_product_links(html_content)))
"""
#save_html_from_url("https://www.tsum.ru/product/7011497-pukhovaya-parka-andrea-campagna-seryi/", "try_find_attributes.html")
response = requests.get("https://www.tsum.ru/product/7015783-kashemirovoe-palto-must-chernyi/", headers = headers)
html_content = response.text
slovar2 = extract_json_from_html(html_content)
print(list(slovar2["product"]["product"]['7015783-kashemirovoe-palto-must-chernyi']["product"].keys()))
for key in slovar2["product"]["product"]['7015783-kashemirovoe-palto-must-chernyi']["product"].keys():
    print(slovar2["product"]["product"]['7015783-kashemirovoe-palto-must-chernyi']["product"][key])
    print(f"{key}////////////////////////////////////////////////////////////////////////////////")


print(slovar2["product"]["product"]['7015783-kashemirovoe-palto-must-chernyi']["product"]["category"]["title"])
print(slovar2["product"]["product"]['7015783-kashemirovoe-palto-must-chernyi']["product"]["information"])
print(slovar2["product"]["product"]['7015783-kashemirovoe-palto-must-chernyi']["product"]["brand"]['title'])
print(slovar2["product"]["product"]['7015783-kashemirovoe-palto-must-chernyi']["product"]["photos"])
image_urls = []
for image in slovar2["product"]["product"]['7015783-kashemirovoe-palto-must-chernyi']["product"]["photos"]:
    image_url = image["large"]
    image_urls.append(image_url)
print(image_urls)
print(slovar2["product"]["product"]['7015783-kashemirovoe-palto-must-chernyi']["product"]["color"]["title"])
print(slovar2["product"]["product"]['7015783-kashemirovoe-palto-must-chernyi']["product"]["sizes"][1]["price"]["originalPrice"])