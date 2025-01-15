import os
import csv


def find_matching_csvs(dir1, dir2):
    """
    Находит CSV-файлы с одинаковыми именами в двух директориях.

    :param dir1: Путь к первой директории.
    :param dir2: Путь ко второй директории.
    :return: Список кортежей пар файлов с одинаковыми именами.
    """
    files1 = set(f for f in os.listdir(dir1) if f.endswith('.csv'))
    files2 = set(f for f in os.listdir(dir2) if f.endswith('.csv'))
    matching_files = files1.intersection(files2)

    dir1_files = [os.path.join(dir1, f) for f in matching_files]
    dir2_files = [os.path.join(dir2, f) for f in matching_files]

    return dir1_files, dir2_files

def compare_and_generate_csv_files(file1, file2, output_csv):
    """
    Сравнивает данные в колонке 'image_url' из первого CSV-файла с колонкой 'image_path' из второго CSV-файла.
    Записывает совпадения в новый CSV.

    :param file1: Путь к первому CSV-файлу.
    :param file2: Путь ко второму CSV-файлу.
    :param output_csv: Путь для сохранения результата.
    """
    with open(file1, 'r', encoding='utf-8') as f1, open(file2, 'r', encoding='utf-8') as f2, open(output_csv, 'w', newline='', encoding='utf-8') as outfile:
        reader1 = csv.DictReader(f1)
        reader2 = csv.DictReader(f2)
        writer = csv.writer(outfile)

        # Заголовки результата
        writer.writerow(['image_path', 'list_categories', 'all_atributes', 'source', 'description', 'Embedding', 'Image_url'])

        rows1 = {}
        rows2 = {}

        # Извлечение ключей из file1
        for row in reader1:
            if 'Image_url' in row and row['Image_url']:
                parts = row['Image_url'].split('/')
                if len(parts) > 6:  # Убедимся, что индекс 6 существует
                    key = parts[6]
                    rows1[key] = row

        # Извлечение ключей из file2
        for row in reader2:
            if 'image_path' in row and row['image_path']:
                parts = row['image_path'].split('\\')
                if len(parts) > 7:  # Убедимся, что индекс 7 существует
                    key = parts[7]
                    rows2[key] = row

        # Ищем совпадения
        common_images = rows1.keys() & rows2.keys()
        print(rows1.keys())
        print(rows2.keys())
        print(common_images)
        # Записываем совпадения в выходной CSV
        for image in common_images:
            row1 = rows1[image]
            row2 = rows2[image]
            writer.writerow([
                row2['image_path'],        # Из второго файла
                row2.get('list_categories', ''),  # Из второго файла
                row2.get('all_atributes', ''),    # Из второго файла
                row2.get('source', ''),           # Из второго файла
                row2.get('description', ''),      # Из второго файла
                row1.get('Embedding', ''),        # Из первого файла
                row1['Image_url']                 # Из первого файла
            ])

    print(f"Результаты записаны в файл: {output_csv}")

def compare_and_generate_csv(dir1, dir2, output_dir):
    """
    Сравнивает данные в колонке 'image_url' из файлов в dir1 с колонкой 'image_path' из файлов в dir2.
    Создает отдельный CSV-файл для каждой пары совпадающих файлов с припиской '_complete'.

    :param dir1: Путь к первой директории.
    :param dir2: Путь ко второй директории.
    :param output_dir: Путь к директории для сохранения результатов.
    """
    # Создаем директорию для результатов, если её нет
    os.makedirs(output_dir, exist_ok=True)

    # Получаем списки совпадающих CSV-файлов
    dir1_files, dir2_files = find_matching_csvs(dir1, dir2)

    for file1, file2 in zip(dir1_files, dir2_files):
        # Имя для выходного файла с припиской '_complete'
        base_name1 = os.path.basename(file1).replace('.csv', '')
        base_name2 = os.path.basename(file2).replace('.csv', '')
        output_csv = os.path.join(output_dir, f"{base_name1}_{base_name2}_complete.csv")

        print(f"Обработка: {file1} и {file2}")
        with open(file1, 'r', encoding='utf-8') as f1, open(file2, 'r', encoding='utf-8') as f2, open(output_csv, 'w', newline='', encoding='utf-8') as outfile:
            reader1 = csv.DictReader(f1)
            reader2 = csv.DictReader(f2)
            writer = csv.writer(outfile)

            # Заголовки результата
            writer.writerow(['image_path', 'list_categories', 'all_atributes', 'source', 'description', 'Embedding', 'Image_url'])

            # Извлекаем данные из колонок
            rows1 = {}
            rows2 = {}

            # Извлечение ключей из file1
            for row in reader1:
                if 'Image_url' in row and row['Image_url']:
                    parts = row['Image_url'].split('/')
                    if len(parts) > 6:  # Убедимся, что индекс 6 существует
                        key = parts[6]
                        rows1[key] = row

            # Извлечение ключей из file2
            for row in reader2:
                if 'image_path' in row and row['image_path']:
                    parts = row['image_path'].split('\\')
                    if len(parts) > 7:  # Убедимся, что индекс 7 существует
                        key = parts[7]
                        rows2[key] = row

            # Ищем совпадения
            common_images = rows1.keys() & rows2.keys()

            # Записываем совпадения в выходной CSV
            for image in common_images:
                row1 = rows1[image]
                row2 = rows2[image]
                writer.writerow([
                    row2['image_path'],        # Из файла dir2
                    row2.get('list_categories', ''),  # Из файла dir2
                    row2.get('all_atributes', ''),    # Из файла dir2
                    row2.get('source', ''),           # Из файла dir2
                    row2.get('description', ''),      # Из файла dir2
                    row1.get('Embedding', ''),        # Из файла dir1
                    row1['Image_url']                 # Из файла dir1
                ])

        print(f"Результаты для файлов {file1} и {file2} записаны в {output_csv}")
    print(f"Готово! Все результаты записаны в директорию: {output_dir}")

def main():
    dir1 = r'D:\DataSet\output_csv_lamoda_WITH_DUPLICATES'
    dir2 = r'D:\Python\Ai\ScraperForFineTuning'
    output_csv = r'D:\DataSet\DataWithEmbedding'

    dir_1_files, dir_2_files = (find_matching_csvs(dir1, dir2))
    print(dir_1_files)
    print(dir_2_files)
    compare_and_generate_csv(dir1, dir2, output_csv)

    temp_dir2 = "withDescription\\Lamoda-Man-accs-mujskie-sredstva-i-aksessuary-dlya-odejdy.csv"
    temp_dir1 = "withEmbedding\\Lamoda-Man-accs-mujskie-sredstva-i-aksessuary-dlya-odejdy.csv"
    csv_output = "Result\\result.csv"
    #compare_and_generate_csv_files(temp_dir1,temp_dir2, csv_output)
    print(f"Результаты записаны в файл: {output_csv}")

if __name__ == '__main__':
    main()
