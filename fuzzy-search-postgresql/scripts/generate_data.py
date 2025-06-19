import psycopg2
import random
from faker import Faker
import uuid

fake = Faker()

categories = ['Electronics', 'Clothing', 'Food', 'Books', 'Sports', 'Home', 'Toys']
brands = ['TechCorp', 'StyleBrand', 'FoodMaster', 'ReadMore', 'SportsPro', 'HomeComfort', 'FunToys']

def introduce_typo(text):
    if len(text) < 2:
        return text
    typo_type = random.choice(['swap', 'delete', 'insert', 'replace'])
    pos = random.randint(0, len(text) - 2)
    if typo_type == 'swap':
        return text[:pos] + text[pos+1] + text[pos] + text[pos+2:]
    elif typo_type == 'delete':
        return text[:pos] + text[pos+1:]
    elif typo_type == 'insert':
        return text[:pos] + random.choice('abcdefghijklmnopqrstuvwxyz') + text[pos:]
    elif typo_type == 'replace':
        return text[:pos] + random.choice('abcdefghijklmnopqrstuvwxyz') + text[pos+1:]
    return text

def generate_products(n=10000):
    products = []
    for _ in range(n):
        name = fake.catch_phrase()
        if random.random() < 0.1:
            name = introduce_typo(name)
        products.append((
            name,
            fake.text(max_nb_chars=200),
            random.choice(categories),
            random.choice(brands),
            f"SKU-{uuid.uuid4().hex[:8].upper()}"
        ))
    return products

def insert_into_db(products):
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        database="fuzzy_search_lab",
        user="postgres",
        password="#*&6tr8GDY@E" #getpass.getpass("Password: ")
    )
    cur = conn.cursor()
    for p in products:
        cur.execute("""
            INSERT INTO products (name, description, category, brand, sku)
            VALUES (%s, %s, %s, %s, %s)
        """, p)
    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    data = generate_products(10000)
    insert_into_db(data)
