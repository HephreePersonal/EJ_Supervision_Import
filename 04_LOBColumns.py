from utils.etl_helpers import execute_sql_with_timeout


def get_max_length(data_type: str, current: int | None = None) -> int:
    return current or 0


def gather_lob_columns(conn, cfg, log_file):
    batch_size = cfg.get("batch_size", 100)
    include_empty = cfg.get("include_empty_tables", False)
    always_include = set(cfg.get("always_include_tables", []))
    cursor = execute_sql_with_timeout(conn, "", timeout=cfg.get("sql_timeout", 30))
    cur = conn.cursor()
    while True:
        rows = cursor.fetchmany(batch_size)
        if not rows:
            break
        to_update = []
        for row in rows:
            schema, table, column, data_type, cur_len, row_cnt = row
            if row_cnt > 0 or include_empty or f"{schema}.{table}" in always_include:
                to_update.append(row)
        if to_update:
            cur.executemany("", to_update)
            conn.commit()
    conn.commit()
