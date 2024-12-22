import numpy as np


# Функция для обновления вектора пользователя на основе градиента и старого состояния
def update_user_vector(user, old_inf, grad, koefs1, koefs2):
    """
    Обновление вектора пользователя на основе градиента и старого состояния.

    :param user: Вектор текущего состояния пользователя.
    :param old_inf: Старый вектор, который содержит информацию о предыдущих изменениях.
    :param grad: Градиент для обновления.
    :param koefs1: Коэффициент для градиента.
    :param koefs2: Коэффициент для старого вектора.
    :return: Новый вектор пользователя и новый старый вектор.
    """
    # Сдвиг по старому состоянию (аналог momentum)
    v = koefs2 * old_inf - koefs1 * grad * (user + koefs2 * old_inf)
    new_user = user + v
    new_old_inf = v

    return new_user, new_old_inf


# Функция для вычисления градиента между текущим вектором и целевым вектором
def compute_gradient(target, current):
    """
    Вычисление градиента, который указывает, в какую сторону нужно двигаться,
    чтобы приблизиться к целевому вектору.
    """
    return target - current


# Пример значений:
user = np.random.rand(1000)  # Вектор пользователя размерности 10 (рандомные значения)
old_inf = np.zeros(1000)  # Старый вектор, изначально нулевой
target =np.random.rand(1000)  # Целевой вектор

koefs1 = 0.2  # Уменьшенный коэффициент для градиента
koefs2 = 0.2  # Уменьшенный коэффициент для старого вектора

user_history = []
user_history.append(user)
# Процесс обучения, пытаемся приблизиться к целевому вектору
for epoch in range(1000):
    grad = compute_gradient(target, user)  # Вычисляем градиент для обновления
    user, old_inf = update_user_vector(user, old_inf, -grad, koefs1, koefs2)  # Обновляем вектор пользователя
    user_history.append(user)
    # Выводим промежуточные результаты
    if epoch % 10 == 0:
        pass
        #print(f"Эпоха {epoch}: {user}")
        #print(f"Градиент: {grad}")

    # Останавливаем, если вектор пользователя достаточно близок к целевому
    if np.linalg.norm(user - target) < 0.01:
        print(f"Достигнут целевой вектор на эпохе {epoch}")
        break

# Финальный результат
print(f"Финальный вектор пользователя: {user}")

