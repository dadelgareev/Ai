from ImageEmbeddingClient import ImageEmbeddingClient
from clientCSV import CsvUploaderClient
from scraper import LamodaScraper
import concurrent.futures

scraper = LamodaScraper()

grpc_client_AI = ImageEmbeddingClient(server_address="localhost:50052")
grpc_client_CSV = CsvUploaderClient(server_address="localhost:50051")

def process_category_test(scraper, main_category, category_info):
    href_list = []

    category_name = category_info.get("category_name", 'Не указано')
    category_url = category_info.get("category_url", 'Не указано')
    category_short_url = category_url.split('/')[3]

    file_name = f'Lamoda-{main_category[1]}-{category_short_url}'
    url = 'https://www.lamoda.ru' + category_url

    # Загружаем ссылки для первой страницы
    for j in range(1, 2):
        href_list = scraper.get_href_list(url, j, href_list)
        print(f"Категория - {category_name}, {j} - страничка была загружена Текущий список ссылок: {len(href_list)}")

    href_list = scraper.remove_duplicates(href_list)

    # Обновляем файл ссылок и CSV для новых ссылок
    scraper.update_links_file_json(f'{file_name}.json', href_list)
    #scraper.create_and_append_csv_json(f'{file_name}.json', f'{file_name}.csv', main_category, grpc_client_AI)
    #grpc_client_CSV.upload_csv(f'{subcategory}.csv')


def process_category(scraper, main_category, category_info):
    try:
        href_list = []

        category_name = category_info.get("category_name", 'Не указано')
        category_url = category_info.get("category_url", 'Не указано')
        category_short_url = category_url.split('/')[3]

        file_name = f'Lamoda-{main_category[1]}-{category_short_url}'
        url = 'https://www.lamoda.ru' + category_url

        # Загружаем ссылки для первой страницы
        for j in range(1, scraper.parse_count_pages(url)+1):
            href_list = scraper.get_href_list(url, j, href_list)
            print(f"Категория - {category_name}, {j} - страничка была загружена")

        href_list = scraper.remove_duplicates(href_list)

        # Обновляем файл ссылок и CSV для новых ссылок
        scraper.update_links_file_json(f'{file_name}.json', href_list)
        scraper.create_and_append_csv_json(f'{file_name}.json', f'{file_name}.csv', main_category, grpc_client_AI)
        # grpc_client_CSV.upload_csv(f'{subcategory}.csv')

    except Exception as e:
        print(f"Ошибка в категории {category_info}: {e}")

def test_one_light_category():
    main_category = scraper.list_categories["man_shoes"]
    category_info = scraper.get_full_width_elements(main_category[0])[0]

    href_list = []

    category_name = category_info.get("category_name", 'Не указано')
    category_url = category_info.get("category_url", 'Не указано')
    category_short_url = category_url.split('/')[3]

    file_name = f'Lamoda-{main_category[1]}-{category_short_url}'
    url = 'https://www.lamoda.ru' + category_url

    # Загружаем ссылки для первой страницы
    for j in range(1, 2):
        href_list = scraper.get_href_list(url, j, href_list)
        print(f"Категория - {category_name}, {j} - страничка была загружена")

    href_list = scraper.remove_duplicates(href_list)

    # Обновляем файл ссылок и CSV для новых ссылок
    scraper.update_links_file_json(f'{file_name}.json', href_list)
    scraper.create_and_append_csv_json(f'{file_name}.json', f'{file_name}.csv', main_category, grpc_client_AI)

def run():
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []

        for main_category in scraper.list_categories:
            categories_list = scraper.get_full_width_elements(scraper.list_categories[main_category][0])
            for category_info in categories_list:
                futures.append(
                    executor.submit(process_category, scraper, scraper.list_categories[main_category], category_info))

        for future in concurrent.futures.as_completed(futures, timeout=7200):  # Устанавливаем тайм-аут
            try:
                print(future.result())
            except concurrent.futures.TimeoutError:
                print("Ошибка: выполнение задачи превысило тайм-аут.")
            except Exception as e:
                print(f"Ошибка при обработке задачи: {e}")

run()