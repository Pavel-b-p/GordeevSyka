-- Включаем расширения
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS fuzzystrmatch;

-- Основная таблица продуктов
CREATE TABLE IF NOT EXISTS products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    category VARCHAR(100),
    brand VARCHAR(100),
    sku VARCHAR(50) UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Таблица для логирования бенчмарков
CREATE TABLE IF NOT EXISTS search_benchmarks (
    id SERIAL PRIMARY KEY,
    method VARCHAR(50),
    dataset_size INTEGER,
    query_text VARCHAR(255),
    execution_time_ms FLOAT,
    result_count INTEGER,
    index_used BOOLEAN,
    test_run_id UUID,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Таблица с тестовыми запросами
CREATE TABLE IF NOT EXISTS test_queries (
    id SERIAL PRIMARY KEY,
    correct_term VARCHAR(255),
    typo_term VARCHAR(255),
    error_type VARCHAR(50)  -- transposition, deletion, insertion, substitution
);
