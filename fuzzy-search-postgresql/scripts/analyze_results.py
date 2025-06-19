import psycopg2
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import getpass
import os

# ─── Подключение к БД ───────────────────────────────────────────────────────────
def load_data():
    host="localhost"
    port=5432
    database="fuzzy_search_lab"
    user="postgres"
    password="#*&6tr8GDY@E" #getpass.getpass("Password: ")

    conn = psycopg2.connect(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password
    )

    df = pd.read_sql("SELECT * FROM search_benchmarks", conn)
    conn.close()
    return df

# ─── График производительности по методам ────────────────────────────────────────
def plot_execution_time(df):
    plt.figure(figsize=(10, 6))
    sns.barplot(data=df, x="method", y="execution_time_ms", errorbar="sd", estimator="mean")
    plt.xticks(rotation=45)
    plt.ylabel("Execution Time (ms)")
    plt.title("Среднее время выполнения по методам")
    plt.tight_layout()
    plt.savefig("../results/performance_chart.png")
    plt.close()

# ─── Точность поиска (если есть эталонные запросы) ───────────────────────────────
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

# ─── Главная точка входа ─────────────────────────────────────────────────────────
if __name__ == "__main__":
    df = load_data()
    print(f"Загружено {len(df)} записей из search_benchmarks")
    plot_execution_time(df)
    plot_precision_table(df)
    print("Графики сохранены в папке /results")
