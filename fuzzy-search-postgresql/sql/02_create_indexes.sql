-- Индексы для LIKE/ILIKE
CREATE INDEX IF NOT EXISTS idx_products_name ON products(name);
CREATE INDEX IF NOT EXISTS idx_products_name_lower ON products(LOWER(name));

-- Индекс триграмм
CREATE INDEX IF NOT EXISTS idx_products_name_trgm ON products USING gin (name gin_trgm_ops);

-- Full-text search
ALTER TABLE products ADD COLUMN IF NOT EXISTS search_vector tsvector;

UPDATE products
SET search_vector = to_tsvector('english',
    coalesce(name, '') || ' ' ||
    coalesce(description, '') || ' ' ||
    coalesce(category, ''));

CREATE INDEX IF NOT EXISTS idx_products_fts ON products USING gin(search_vector);
