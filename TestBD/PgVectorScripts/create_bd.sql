-- Создание схемы
CREATE SCHEMA ecommerce;

-- 1. Таблица sources
CREATE TABLE ecommerce.sources (
    id SERIAL PRIMARY KEY,
    source_name VARCHAR(255) NOT NULL
);

-- 2. Таблица categories
CREATE TABLE ecommerce.categories (
    id SERIAL PRIMARY KEY,
    category_name VARCHAR(255) NOT NULL
);

-- 3. Таблица subcategories
CREATE TABLE ecommerce.subcategories (
    id SERIAL PRIMARY KEY,
    subcategory_name VARCHAR(255) NOT NULL
);

-- 4. Таблица category_links
CREATE TABLE ecommerce.category_links (
    id SERIAL PRIMARY KEY,
    category_id INT REFERENCES ecommerce.categories(id),
    subcategory_id INT REFERENCES ecommerce.subcategories(id),
    UNIQUE (category_id, subcategory_id) -- Гарантия уникальности связи
);

-- 5. Таблица brands
CREATE TABLE ecommerce.brands (
    id SERIAL PRIMARY KEY,
    brand_name VARCHAR(255) NOT NULL
);

-- 6. Таблица genders
CREATE TABLE ecommerce.genders (
    id SERIAL PRIMARY KEY,
    gender_name VARCHAR(255) NOT NULL
);

-- 7. Таблица cards (после создания всех зависимостей)
CREATE TABLE ecommerce.cards (
    id UUID PRIMARY KEY,
    source_id INT REFERENCES ecommerce.sources(id),
    article VARCHAR(63) NOT NULL,
    price NUMERIC(10, 2),
    brand_id INT REFERENCES ecommerce.brands(id),
    gender_id INT REFERENCES ecommerce.genders(id),
    category_link_id INT REFERENCES ecommerce.category_links(id), -- Ссылка на category_links
    tags JSONB,
    title VARCHAR(255) NOT NULL
);

-- 8. Таблица images (ссылается на cards)
CREATE TABLE ecommerce.images (
    id SERIAL PRIMARY KEY,
    card_id UUID REFERENCES ecommerce.cards(id),
    image_url TEXT NOT NULL,
    main_photo BOOLEAN DEFAULT FALSE
    vector VECTOR(1000) NOT NULL
);

