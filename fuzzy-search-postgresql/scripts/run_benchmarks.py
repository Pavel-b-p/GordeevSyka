import psycopg2
import time
import uuid
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os


class FuzzySearchBenchmark:
    def __init__(self, connection_params):
        self.conn = psycopg2.connect(**connection_params)
        self.test_run_id = str(uuid.uuid4())

    def benchmark_method(self, method_name, query_template, search_term, dataset_size, index_used=False):
        cursor = self.conn.cursor()

        #cursor.execute(f"""
        #    CREATE OR REPLACE TEMP VIEW products_subset AS
        #    SELECT * FROM products LIMIT {dataset_size}
        #""")

        cursor.execute("CREATE OR REPLACE TEMP VIEW products_subset AS SELECT * FROM products")

        start_time = time.time()
        cursor.execute(query_template.format(term=search_term))
        results = cursor.fetchall()
        execution_time = (time.time() - start_time) * 1000  # ms

        cursor.execute("""
            INSERT INTO search_benchmarks 
            (method, dataset_size, query_text, execution_time_ms, result_count, index_used, test_run_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            method_name, dataset_size, search_term,
            execution_time, len(results), index_used, self.test_run_id
        ))
        self.conn.commit()
        cursor.close()
        return execution_time, len(results)

    def load_results(self):
        return pd.read_sql(
            "SELECT * FROM search_benchmarks WHERE test_run_id = %s",
            self.conn,
            params=(self.test_run_id,)
        )

    def close(self):
        self.conn.close()

def plot_execution_time(df):
    plt.figure(figsize=(10, 6))
    sns.barplot(data=df, x="method", y="execution_time_ms", errorbar="sd", estimator="mean")
    plt.xticks(rotation=45)
    plt.ylabel("Execution Time (ms)")
    plt.title("Среднее время выполнения по методам")
    plt.tight_layout()
    plt.savefig("../results/performance_chart.png")
    plt.close()

def plot_precision_table(df):
    precision_table = (
        df.groupby("method")["result_count"]
            .mean()
            .reset_index()
            .rename(columns={"result_count": "avg_results"})
    )
    print("\nСреднее число результатов по методам:")
    print(precision_table)

    plt.figure(figsize=(10, 6))
    sns.barplot(data=precision_table, x="method", y="avg_results")
    plt.xticks(rotation=45)
    plt.title("Среднее количество найденных результатов")
    plt.ylabel("Avg. Result Count")
    plt.tight_layout()
    plt.savefig("../results/avg_result_count.png")
    plt.close()

if __name__ == "__main__":
    connection_params = {
        "host": "localhost",
        "port": 5432,
        "database": "fuzzy_search_lab",
        "user": "postgres",
        "password": "#*&6tr8GDY@E"
    }

    benchmark = FuzzySearchBenchmark(connection_params)

    # 🔍 Список методов, которые мы будем тестировать
    search_term = "laptop"
    dataset_size = 20003

    methods = [
        {
            "name": "ILIKE",
            "query": "SELECT * FROM products_subset WHERE name ILIKE '%{term}%'",
            "index_used": False
        },
        {
            "name": "Trigram Similarity",
            "query": "SELECT * FROM products_subset WHERE similarity(name, '{term}') > 0.3 ORDER BY similarity(name, '{term}') DESC",
            "index_used": True
        },
        {
            "name": "Soundex",
            "query": "SELECT * FROM products_subset WHERE soundex(name) = soundex('{term}')",
            "index_used": True
        },
        {
            "name": "Levenshtein <= 2",
            "query": "SELECT * FROM products_subset WHERE levenshtein(name, '{term}') <= 2",
            "index_used": False
        },
        {
            "name": "FTS",
            "query": "SELECT * FROM products_subset WHERE to_tsvector('english', name) @@ plainto_tsquery('english', '{term}')",
            "index_used": True
        }
    ]

    for method in methods:
        print(f"⏳ Running: {method['name']} ...")
        exec_time, result_count = benchmark.benchmark_method(
            method_name=method["name"],
            query_template=method["query"],
            search_term=search_term,
            dataset_size=dataset_size,
            index_used=method["index_used"]
        )
        print(f"✅ Done: {method['name']} — {exec_time:.2f}ms, {result_count} results")

    # 📊 Анализ и графики
    df = benchmark.load_results()
    print(f"\n📥 Загружено {len(df)} записей из search_benchmarks")

    if not df.empty:
        plot_execution_time(df)
        plot_precision_table(df)
        print("📈 Графики сохранены в папке /results")

    benchmark.close()
