-- Создать схему user, если не существует
CREATE SCHEMA IF NOT EXISTS user;

-- Создать таблицу user
CREATE TABLE IF NOT EXISTS user.user (
    id SERIAL PRIMARY KEY,
    username VARCHAR(255) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Создать таблицу user_vector
CREATE TABLE IF NOT EXISTS user.user_vector (
    id INT PRIMARY KEY,
    vector_1 vector(1000),
    vector_2 vector(1000),
    vector_3 vector(1000),
    vector_4 vector(1000),
    vector_5 vector(1000),
    vector_6 vector(1000),
    vector_7 vector(1000),
    vector_8 vector(1000),
    vector_9 vector(1000),
    vector_10 vector(1000),
    vector_11 vector(1000),
    vector_12 vector(1000),
    vector_13 vector(1000),
    vector_14 vector(1000),
    vector_15 vector(1000),
    vector_16 vector(1000),
    vector_17 vector(1000),
    vector_18 vector(1000),
    vector_19 vector(1000),
    vector_20 vector(1000),
    vector_21 vector(1000),
    vector_22 vector(1000),
    vector_23 vector(1000),
    vector_24 vector(1000),
    vector_25 vector(1000),
    vector_26 vector(1000),
    vector_27 vector(1000),
    vector_28 vector(1000),
    vector_29 vector(1000),
    vector_30 vector(1000),
    vector_31 vector(1000),
    vector_32 vector(1000),
    vector_33 vector(1000),
    vector_34 vector(1000),
    CONSTRAINT fk_user FOREIGN KEY (id) REFERENCES user.user (id) ON DELETE CASCADE
);
