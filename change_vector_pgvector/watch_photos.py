from main import user_training, fetch_similar_product_for_user_with_ids
conn_params = {
    'host': '193.232.55.5',
    'database': 'postgres',
    'port': 5500,
    'user': 'postgres',
    'password': 'super'
}
import psycopg2
import tkinter as tk
from tkinter import messagebox
import requests
from PIL import Image, ImageTk
from io import BytesIO
import json
import concurrent.futures
category = "Блузы и рубашки"
gender = "Man"



# Эта функция будет вызываться для загрузки изображений по URL
def load_image(url, callback):
    """
    Загружает изображение по URL и передает результат через callback.

    Args:
        url (str): URL изображения.
        callback (function): Функция, которая будет вызвана после загрузки изображения с аргументом image.
    """
    try:
        response = requests.get(url)
        img_data = response.content
        img = Image.open(BytesIO(img_data))
        img = img.resize((100, 100))  # Устанавливаем размер изображения
        print(url, 'Успешно')
        callback(img)
    except Exception as e:
        print(f"Ошибка при загрузке изображения {url}: {e}")
        callback(None)
def get_links_from_db(category, gender, page, limit=100):
    """
    Извлекает 100 ссылок из таблицы products_all с учетом категории, пола и страницы.

    Args:
        category (str): Категория для фильтрации.
        gender (str): Пол для фильтрации.
        page (int): Номер страницы для извлечения ссылок.
        limit (int, optional): Количество ссылок для вывода. По умолчанию 100.

    Returns:
        list: Список ссылок для указанной страницы.
    """
    offset = (page - 1) * limit  # Вычисляем смещение для пагинации

    # SQL-запрос для выборки ссылок
    query = """
        SELECT image_url 
        FROM products_all 
        WHERE category = %s AND gender = %s
        LIMIT %s OFFSET %s;
    """

    try:
        # Подключаемся к базе данных
        connection = psycopg2.connect(**conn_params)
        cursor = connection.cursor()

        # Выполняем запрос
        cursor.execute(query, (category, gender, limit, offset))

        # Извлекаем все результаты
        result = cursor.fetchall()

        # Преобразуем результат в список ссылок
        links = [row[0] for row in result]

        cursor.close()
        connection.close()

        return links

    except psycopg2.Error as e:
        print(f"Ошибка при работе с базой данных: {e}")
        return []

def fetch_similar_product_links_for_user(user_id, conn_params, category, gender, similarity_threshold):
    """
    Извлекает список ссылок на изображения похожих товаров для пользователя по его preferences_embedding_1,
    игнорируя идентификаторы товаров, уже просмотренные пользователем (viewed_ids).

    :param user_id: ID пользователя.
    :param conn_params: Параметры подключения к базе данных.
    :param category: Категория товара, по которой будет выполняться поиск.
    :param gender: Пол, по которому будет выполняться поиск (например, 'Man' или 'Woman').
    :param similarity_threshold: Порог сходства, ниже которого товары не будут учитываться.
    :return: Список ссылок на изображения товаров (или пустой список, если подходящих товаров нет).
    """
    # Подключение к базе данных
    conn = psycopg2.connect(**conn_params)
    cursor = conn.cursor()

    # Запрос для нахождения похожих товаров и извлечения embedding пользователя,
    # исключая идентификаторы из viewed_ids.
    query = """
    WITH user_embedding AS (
        SELECT 
            preferences_embedding_1, 
            COALESCE(viewed_ids, '{}') AS viewed_ids
        FROM user_info
        WHERE user_id = %s
    )
    SELECT 
        p.image_url
    FROM 
        products_all p,
        user_embedding ue
    WHERE 
        p.category = %s
        AND p.gender = %s
        AND (1 - (ue.preferences_embedding_1 <=> p.embedding)) >= %s
        AND p.id::text <> ALL(ue.viewed_ids) -- Исключаем идентификаторы из viewed_ids
    ORDER BY 
        (1 - (ue.preferences_embedding_1 <=> p.embedding)) DESC
    LIMIT 100;
    """

    # Выполнение запроса с параметрами для категории, гендера и порога сходства
    cursor.execute(query, (user_id, category, gender, similarity_threshold))
    result = cursor.fetchall()

    # Извлечение ссылок на изображения из результата
    links = [row[0] for row in result] if result else []

    # Закрытие соединения
    cursor.close()
    conn.close()

    return links


def display_images(url_list, page, category, gender):
    """
    Создает окно Tkinter и выводит изображения, полученные по URL.

    Args:
        url_list (list): Список URL-адресов изображений.
        page (int): Текущая страница.
        category (str): Категория товаров.
        gender (str): Пол.
    """
    # Создаем окно Tkinter
    window = tk.Tk()
    window.title("Изображения")

    # Количество изображений на строку
    images_per_row = 15
    padding = 10  # Отступ между изображениями

    # Функция для обновления отображения изображений в GUI
    def update_display(images):
        # Очистка старых изображений
        for widget in frame.winfo_children():
            widget.destroy()

        # Отображаем изображения
        row, col = 0, 0  # Индексы для размещения изображений
        for img in images:
            label = tk.Label(frame, image=img)
            label.image = img  # Сохраняем ссылку на изображение, иначе оно не будет отображаться
            label.grid(row=row, column=col, padx=padding, pady=padding)
            col += 1
            if col >= images_per_row:
                col = 0
                row += 1

        page_label.config(text=f"Страница: {page}")

    # Функция для загрузки изображений асинхронно
    def load_images_concurrently(urls):
        images = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            future_to_url = {executor.submit(load_image, url, lambda img: images.append(img)): url for url in urls}
            for future in concurrent.futures.as_completed(future_to_url):
                pass
        # После загрузки всех изображений обновляем интерфейс
        images = [ImageTk.PhotoImage(img) for img in images if img]
        window.after(0, update_display, images)  # Вызываем обновление в основном потоке

    # Создаем контейнер для изображений
    frame = tk.Frame(window)
    frame.pack()

    # Получаем изображения
    load_images_concurrently(url_list)

    # Кнопки для навигации
    def prev_page():
        nonlocal page
        if page > 1:
            page -= 1
            links = get_links_from_db(category, gender, page)
            load_images_concurrently(links)

    def next_page():
        nonlocal page
        page += 1
        links = get_links_from_db(category, gender, page)
        load_images_concurrently(links)

    prev_button = tk.Button(window, text="Влево", command=prev_page)
    prev_button.pack(side=tk.LEFT, padx=10, pady=10)

    next_button = tk.Button(window, text="Вправо", command=next_page)
    next_button.pack(side=tk.RIGHT, padx=10, pady=10)

    # Метка для отображения номера текущей страницы
    page_label = tk.Label(window, text=f"Страница: {page}")
    page_label.pack(pady=20)

    # Запускаем основной цикл окна
    window.mainloop()
def like_or_dislike_mini_app(user_id, category, gender, conn_params):
    """
    Создаёт мини-приложение для лайков/дизлайков товаров.

    :param user_id: ID пользователя.
    :param category: Категория товаров.
    :param gender: Пол (например, 'Man' или 'Woman').
    :param conn_params: Параметры подключения к базе данных.
    """
    def fetch_new_product():
        """
        Извлекает новый товар для отображения.
        """
        nonlocal similar_id, similar_embedding, similar_image_url
        result = fetch_similar_product_for_user_with_ids(user_id, conn_params, category, gender, 0.8)
        if result:
            similar_id, similar_embedding, similar_image_url = result
            update_image(similar_image_url)
        else:
            tk.messagebox.showinfo("Конец", "Больше товаров нет.")
            window.destroy()

    def update_image(image_url):
        """
        Загружает и обновляет изображение товара в интерфейсе.
        """
        try:
            response = requests.get(image_url)
            response.raise_for_status()
            img_data = BytesIO(response.content)
            pil_image = Image.open(img_data).resize((500, 500))  # Меняем размер
            tk_image = ImageTk.PhotoImage(pil_image)
            image_label.config(image=tk_image)
            image_label.image = tk_image  # Сохраняем ссылку, чтобы изображение не очищалось
        except Exception as e:
            tk.messagebox.showerror("Ошибка", f"Не удалось загрузить изображение: {e}")

    def handle_like():
        """
        Обработка события "Like".
        """
        print(f"Пользователь {user_id} лайкнул товар ID {similar_id}.")
        user_training(user_id,category,gender,0.01,0.9, True,conn_params)
        fetch_new_product()
        window.after(0, update_image, similar_image_url)

    def handle_dislike():
        """
        Обработка события "Dislike".
        """
        print(f"Пользователь {user_id} дизлайкнул товар ID {similar_id}.")
        user_training(user_id, category, gender, 0.01, 0.9, False, conn_params)
        fetch_new_product()
        window.after(0, update_image, similar_image_url)

    # Создаем окно Tkinter
    window = tk.Tk()
    window.title("Dressy Demo")

    # Контейнер для изображения
    image_label = tk.Label(window)
    image_label.pack(pady=20)

    # Инициализация первой итерации товара
    similar_id, similar_embedding, similar_image_url = None, None, None
    fetch_new_product()

    # Кнопки для взаимодействия
    dislike_button = tk.Button(window, text="Dislike", command=handle_dislike, bg="red", fg="white", padx=20, pady=10)
    dislike_button.pack(side=tk.LEFT, padx=10, pady=10)

    like_button = tk.Button(window, text="Like", command=handle_like, bg="green", fg="white", padx=20, pady=10)
    like_button.pack(side=tk.RIGHT, padx=10, pady=10)

    # Запуск интерфейса
    window.mainloop()



page = 1
limit = 100
#links = get_links_from_db(category, gender, page)
links = fetch_similar_product_links_for_user(4,conn_params, category, gender, 0.8)
print(links)
#display_images(links,1, category, gender)
#user_training(4,category,gender,0.01,0.9,True,conn_params)
#similar_id, similar_embedding, similar_image_url = fetch_similar_product_for_user_with_ids(4,conn_params, category, gender, 0.8)
#print(similar_id)
like_or_dislike_mini_app(4,category,gender,conn_params)
