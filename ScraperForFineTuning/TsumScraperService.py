from ImageEmbeddingClient import ImageEmbeddingClient
#from clientCSV import CsvUploaderClient
from scraperTsum import TsumScraper
import concurrent.futures

scraper = TsumScraper()

grpc_client_AI = ImageEmbeddingClient(server_address="localhost:50052")
#grpc_client_CSV = CsvUploaderClient(server_address="localhost:50051")

def process_category_test(scraper, main_category, subcategory_dict):
    try:
        href_list = []
        subcategory_short_name = subcategory_dict["url"].split('/')[-2]
        subcategory_russian_title = subcategory_dict["name"]

        url = 'https://www.tsum.ru' + subcategory_dict["url"]

        for j in range(1, 2):
            href_list = scraper.get_href_list(url, j, href_list)
            print(
                f"Категория - {subcategory_short_name}, {j} - страничка была загружена Текущий список ссылок: {len(href_list)}")
        href_list = scraper.remove_duplicates(href_list)
        scraper.update_links_file_json(f"Tsum-{main_category[1]}-{subcategory_short_name}.json", href_list)
        scraper.create_and_append_csv_json(
            f"Tsum-{scraper.list_categories[main_category][1]}-{subcategory_short_name}.json",
            f"Tsum-{scraper.list_categories[main_category][1]}-{subcategory_short_name}.csv",
            scraper.list_categories[main_category], grpc_client_AI)
        print(f'{subcategory_short_name}.json, создан!')
        # grpc_client_CSV.upload_csv(f"Tsum-{scraper.list_categories[main_category][1]}-{subcategory_short_url}.csv")

    except Exception as e:
        print(f"Ошибка в категории {subcategory_short_name}: {e}")


def process_category(scraper, main_category, subcategory_dict):
    try:
        href_list = []
        subcategory_short_name = subcategory_dict["url"].split('/')[-2]
        subcategory_russian_title = subcategory_dict["name"]

        url = 'https://www.tsum.ru' + subcategory_dict["url"]

        for j in range(1, scraper.parse_count_pages(url, subcategory_short_name)):
            href_list = scraper.get_href_list(url, j, href_list)
            print(f"Категория - {subcategory_short_name}, {j} - страничка была загружена Текущий список ссылок: {len(href_list)}")
        href_list = scraper.remove_duplicates(href_list)
        scraper.update_links_file_json(f"Tsum-{main_category[1]}-{subcategory_short_name}.json",href_list)
        print(f'{subcategory_short_name}.json, создан!')
        scraper.create_and_append_csv_json(f"Tsum-{scraper.list_categories[main_category][1]}-{subcategory_short_name}.json",
            f"Tsum-{scraper.list_categories[main_category][1]}-{subcategory_short_name}.csv",
            scraper.list_categories[main_category], grpc_client_AI)

        #grpc_client_CSV.upload_csv(f"Tsum-{scraper.list_categories[main_category][1]}-{subcategory_short_url}.csv")

    except Exception as e:
        print(f"Ошибка в категории {subcategory_short_name}: {e}")

def process_category_fine_tuning(scraper, main_category, subcategory_dict):
    try:
        href_list = []
        subcategory_short_name = subcategory_dict["url"].split('/')[-2]
        subcategory_russian_title = subcategory_dict["name"]

        url = 'https://www.tsum.ru' + subcategory_dict["url"]

        for j in range(1, scraper.parse_count_pages(url, subcategory_short_name)):
            href_list = scraper.get_href_list(url, j, href_list)
            print(f"Категория - {subcategory_short_name}, {j} - страничка была загружена Текущий список ссылок: {len(href_list)}")
        href_list = scraper.remove_duplicates(href_list)
        scraper.update_links_file_json(f"Tsum-{main_category[1]}-{subcategory_short_name}.json",href_list)
        print(f'{subcategory_short_name}.json, создан!')
        scraper.create_and_append_csv_json_fine_tuning(f"Tsum-{scraper.list_categories[main_category][1]}-{subcategory_short_name}.json",
            f"Tsum-{scraper.list_categories[main_category][1]}-{subcategory_short_name}.csv",
            scraper.list_categories[main_category], grpc_client_AI)

        #grpc_client_CSV.upload_csv(f"Tsum-{scraper.list_categories[main_category][1]}-{subcategory_short_url}.csv")

    except Exception as e:
        print(f"Ошибка в категории {subcategory_short_name}: {e}")

def test_one_light_category():
    main_category = scraper.list_categories["man_shoes"]
    category_info = scraper.extract_categories(main_category[0])[0]

    href_list = []

    category_name = category_info.get("name", 'Не указано')
    category_url = category_info.get("url", 'Не указано')
    category_short_url = category_url.split('/')[2]
    print(category_short_url)

    file_name = f'Tsum-{main_category[1]}-{category_short_url}'
    url = 'https://www.tsum.ru' + category_url

    # Загружаем ссылки для первой страницы
    for j in range(1, 2):
        href_list = scraper.get_href_list(url, j, href_list)
        print(f"Категория - {category_name}, {j} - страничка была загружена")

    href_list = scraper.remove_duplicates(href_list)

    # Обновляем файл ссылок и CSV для новых ссылок
    scraper.update_links_file_json(f'{file_name}.json', href_list)
    print(f'{file_name}.json, создан!')
    scraper.create_and_append_csv_json(f'{file_name}.json', f'{file_name}.csv', main_category, grpc_client_AI)

def run():
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        futures = []

        for main_category in scraper.list_categories:
            info = scraper.extract_categories(scraper.list_categories[main_category][0])
            for subcategory_dict in info:
                futures.append(
                    executor.submit(process_category_fine_tuning, scraper, scraper.list_categories[main_category], subcategory_dict))

        for future in concurrent.futures.as_completed(futures, timeout=7200):  # Устанавливаем тайм-аут
            try:
                print(future.result())
            except concurrent.futures.TimeoutError:
                print("Ошибка: выполнение задачи превысило тайм-аут.")
            except Exception as e:
                print(f"Ошибка при обработке задачи: {e}")


def test_fine_tuning_generate_csv():
    main_category = scraper.list_categories["man_shoes"]
    category_info = scraper.extract_categories(main_category[0])[0]

    href_list = []

    category_name = category_info.get("name", 'Не указано')
    category_url = category_info.get("url", 'Не указано')
    category_short_url = category_url.split('/')[2]

    file_name = f'Tsum-{main_category[1]}-{category_short_url}'
    url = 'https://www.tsum.ru' + category_url

    # Загружаем ссылки для первой страницы
    for j in range(1, 2):
        href_list = scraper.get_href_list(url, j, href_list)
        print(f"Категория - {category_name}, {j} - страничка была загружена")

    href_list = scraper.remove_duplicates(href_list)

    # Обновляем файл ссылок и CSV для новых ссылок
    scraper.update_links_file_json(f'{file_name}.json', href_list)
    print(f'{file_name}.json, создан!')
    scraper.create_and_append_csv_json_fine_tuning(f'{file_name}.json', f'{file_name}.csv', main_category, grpc_client_AI)

def only_test_run():
    href_list = []
    info = scraper.extract_categories(scraper.list_categories["man_shoes"][0])
    subcategory_dict = info[0]
    subcategory_short_name = subcategory_dict["url"].split('/')[-2]
    print(info)
    print(subcategory_dict)
    subcategory_short_url = subcategory_dict['url']
    url = 'https://www.tsum.ru' + subcategory_short_url
    count = scraper.parse_count_pages(url, subcategory_short_name)
    print(count)
    print(type(count))

    """
    href_list = scraper.get_href_list(url, 1, href_list)
    href_list = scraper.get_href_list(url, 2, href_list)
    print(href_list)
    scraper.update_links_file_json(f"Tsum-{scraper.list_categories["man_shoes"][1]}-{subcategory_short_name}.json",
                                   href_list)
    scraper.create_and_append_csv_json(
        f"Tsum-{scraper.list_categories["man_shoes"][1]}-{subcategory_short_name}.json",
        f"Tsum-{scraper.list_categories["man_shoes"][1]}-{subcategory_short_name}.csv",
        scraper.list_categories["man_shoes"],grpc_client_AI)
    """

