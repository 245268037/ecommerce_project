from pathlib import Path
import sqlite3

import pandas as pd


# ==============================
# 1.确定项目路径
# ==============================

SCRIPT_FILE = Path(__file__).resolve()
PROJECT_DIR = SCRIPT_FILE.parent.parent

RAW_DIR = PROJECT_DIR / "data" / "raw"
DATABASE_DIR = PROJECT_DIR / "output" / "sql"
DATABASE_FILE = DATABASE_DIR / "ecommerce.db"


# ==============================
# 2.定义CSV文件与数据库表关系
# ==============================

TABLE_CONFIG = {
    "orders": RAW_DIR / "order.csv",
    "order_detail": RAW_DIR / "order_detail.csv",
    "customer": RAW_DIR / "customer.csv",
    "product": RAW_DIR / "product.csv",
}


def validate_files():
    """检查所有输入文件是否存在并且非空。"""

    failed_count = 0

    for table_name, file_path in TABLE_CONFIG.items():
        if not file_path.is_file():
            print(
                f"[失败] {table_name}："
                f"文件不存在：{file_path}"
            )
            failed_count += 1
            continue

        if file_path.stat().st_size == 0:
            print(
                f"[失败] {table_name}："
                f"文件为空：{file_path}"
            )
            failed_count += 1
            continue

        print(
            f"[通过] {table_name}：{file_path}"
        )

    if failed_count > 0:
        raise FileNotFoundError(
            f"输入文件检查失败，异常文件数：{failed_count}"
        )


def load_tables(connection):
    """将CSV数据加载到SQLite数据库。"""

    for table_name, file_path in TABLE_CONFIG.items():
        print(
            f"\n开始加载表：{table_name}"
        )

        df = pd.read_csv(
            file_path,
            low_memory=False
        )

        df.to_sql(
            name=table_name,
            con=connection,
            if_exists="replace",
            index=False,
            chunksize=10000
        )

        row_count = connection.execute(
            f"SELECT COUNT(*) FROM {table_name}"
        ).fetchone()[0]

        print(
            f"[完成] {table_name}，"
            f"CSV行数={len(df)}，"
            f"数据库行数={row_count}"
        )


def create_indexes(connection):
    """为后续关联练习创建索引。"""

    index_sql_list = [
        """
        CREATE UNIQUE INDEX IF NOT EXISTS
            idx_orders_order_id
        ON orders(order_id)
        """,
        """
        CREATE UNIQUE INDEX IF NOT EXISTS
            idx_order_detail_detail_id
        ON order_detail(order_detail_id)
        """,
        """
        CREATE INDEX IF NOT EXISTS
            idx_order_detail_order_id
        ON order_detail(order_id)
        """,
        """
        CREATE UNIQUE INDEX IF NOT EXISTS
            idx_customer_customer_id
        ON customer(customer_id)
        """,
        """
        CREATE UNIQUE INDEX IF NOT EXISTS
            idx_product_product_id
        ON product(product_id)
        """
    ]

    for sql in index_sql_list:
        connection.execute(sql)

    connection.commit()

    print("\n索引创建完成")


def show_table_summary(connection):
    """输出数据库表的数据量。"""

    print("\n========== SQLite数据表汇总 ==========")

    for table_name in TABLE_CONFIG:
        row_count = connection.execute(
            f"SELECT COUNT(*) FROM {table_name}"
        ).fetchone()[0]

        print(
            f"{table_name:<15} {row_count:>10} 行"
        )


def main():
    print("开始创建SQL学习数据库")

    DATABASE_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    validate_files()

    with sqlite3.connect(DATABASE_FILE) as connection:
        load_tables(connection)
        create_indexes(connection)
        show_table_summary(connection)

    print(
        f"\n数据库创建完成：{DATABASE_FILE}"
    )


if __name__ == "__main__":
    main()
