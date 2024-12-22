from sqlalchemy import create_engine,text, Column, Integer, String
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import UserDefinedType
from sqlalchemy.orm import declarative_base
class Vector(UserDefinedType):
    def __init__(self, dimension):
        self.dimension = dimension

    def get_col_spec(self):
        # Указываем тип данных как vector(N), где N — размерность
        return f"vector({self.dimension})"

    def bind_processor(self, dialect):
        # Преобразование Python-данных перед сохранением в БД
        def process(value):
            if value is not None and isinstance(value, list):
                return f"[{', '.join(map(str, value))}]"  # Преобразуем список в формат PostgreSQL vector
            return value
        return process

    def result_processor(self, dialect, coltype):
        # Преобразование данных из БД в Python-объекты
        def process(value):
            if value is not None:
                # Удаляем квадратные скобки и разбиваем строку на элементы
                return list(map(float, value.strip("[]").split(", ")))
            return value
        return process

Base = declarative_base()

class MyTable(Base):
    __tablename__ = "test_vector_orm"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)
    embedding = Column(Vector(4), nullable=False)  # Указываем размерность вектора

# Параметры подключения
conn_params = {
    'host': '193.232.55.5',
    'database': 'postgres',
    'port': 5500,
    'user': 'postgres',
    'password': 'super'
}


# Формирование строки подключения
db_url = f"postgresql+psycopg2://{conn_params['user']}:{conn_params['password']}@{conn_params['host']}:{conn_params['port']}/{conn_params['database']}"
engine = create_engine(db_url)
def test_connection():
    try:
        with engine.connect() as connection:
            # Используйте text() для выполнения plain SQL
            result = connection.execute(text("SELECT version();"))
            for row in result:
                print(f"Connected to PostgreSQL database: {row[0]}")
    except Exception as e:
        print(f"Error connecting to PostgreSQL database: {e}")

def get_table_structure(table_name):
    query = text("""
        SELECT 
            column_name,
            data_type,
            character_maximum_length,
            is_nullable
        FROM 
            information_schema.columns
        WHERE 
            table_name = :table_name
    """)
    with engine.connect() as connection:
        result = connection.execute(query, {"table_name": table_name})
        columns = result.keys()  # Имена колонок результата
        return [dict(zip(columns, row)) for row in result.fetchall()]  # Создаем словарь для каждой строки

# Подключение
Session = sessionmaker(bind=engine)
session = Session()

# Создание записи
new_record = MyTable(name="example", embedding=[0.1, 0.2, 0.3, 0.4])
session.add(new_record)
session.commit()

print("Запись добавлена!")

table_name = "products_all"
structure = get_table_structure(table_name)
print(structure)