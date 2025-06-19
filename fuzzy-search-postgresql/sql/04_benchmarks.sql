-- View: обобщённая статистика по методам
DROP VIEW IF EXISTS benchmark_summary CASCADE;

CREATE VIEW benchmark_summary AS
SELECT
    method,
    COUNT(*) AS total_runs,
    ROUND(AVG(execution_time_ms), 2) AS avg_time_ms,
    ROUND(STDDEV(execution_time_ms), 2) AS stddev_time_ms,
    ROUND(AVG(result_count), 2) AS avg_result_count,
    COUNT(DISTINCT test_run_id) AS test_runs
FROM search_benchmarks
GROUP BY method
ORDER BY avg_time_ms;

-- View: подробный лог последних бенчмарков
DROP VIEW IF EXISTS benchmark_detailed_log CASCADE;

CREATE VIEW benchmark_detailed_log AS
SELECT
    id,
    method,
    dataset_size,
    query_text,
    ROUND(execution_time_ms, 2) AS time_ms,
    result_count,
    test_run_id,
    created_at
FROM search_benchmarks
ORDER BY created_at DESC;

-- Function: очистка бенчмарков
DROP FUNCTION IF EXISTS clear_benchmarks CASCADE;

CREATE OR REPLACE FUNCTION clear_benchmarks()
RETURNS void AS $$
BEGIN
    DELETE FROM search_benchmarks;
    RAISE NOTICE 'Все записи из search_benchmarks удалены.';
END;
$$ LANGUAGE plpgsql;

-- Function: подсчёт средних метрик (ручной вызов)
DROP FUNCTION IF EXISTS print_benchmark_stats CASCADE;

CREATE OR REPLACE FUNCTION print_benchmark_stats()
RETURNS TABLE (
    method TEXT,
    avg_time_ms FLOAT,
    avg_result_count FLOAT,
    total_runs INT
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        method,
        ROUND(AVG(execution_time_ms), 2),
        ROUND(AVG(result_count), 2),
        COUNT(*)
    FROM search_benchmarks
    GROUP BY method
    ORDER BY avg_time_ms;
END;
$$ LANGUAGE plpgsql;
