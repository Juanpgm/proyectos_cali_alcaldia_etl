-- Vistas analíticas ultra seguras
-- Generado: 2025-09-19 14:32:10

-- Vista de resumen de tablas (CORREGIDA)
CREATE OR REPLACE VIEW warehouse_summary AS
SELECT 
    schemaname,
    relname as table_name,
    n_tup_ins as total_insertions,
    n_tup_upd as total_updates,
    n_tup_del as total_deletions,
    n_live_tup as live_rows,
    n_dead_tup as dead_rows
FROM pg_stat_user_tables
WHERE schemaname = 'public'
  AND relname NOT LIKE 'pg_%'
ORDER BY relname;

-- Vista de actividad por tabla (CORREGIDA)
CREATE OR REPLACE VIEW table_activity AS
SELECT 
    relname as table_name,
    seq_scan,
    seq_tup_read,
    idx_scan,
    idx_tup_fetch,
    n_live_tup as current_rows
FROM pg_stat_user_tables
WHERE schemaname = 'public'
  AND relname NOT LIKE 'pg_%'
ORDER BY (seq_tup_read + COALESCE(idx_tup_fetch, 0)) DESC;

-- Conteo rápido de todas las tablas
CREATE OR REPLACE VIEW tables_row_count AS
SELECT 
    relname as table_name,
    n_live_tup as row_count,
    pg_size_pretty(pg_total_relation_size(oid)) as table_size
FROM pg_stat_user_tables
WHERE schemaname = 'public'
  AND relname NOT LIKE 'pg_%'
ORDER BY n_live_tup DESC;
