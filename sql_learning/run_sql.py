from pathlib import Path
import sqlite3
import sys


# ==============================
# 1.项目路径
# ==============================

SCRIPT_FILE = Path(__file__).resolve()
PROJECT_DIR = SCRIPT_FILE.parent.parent

DATABASE_FILE = (
    PROJECT_DIR
    / "output"
    / "sql"
    / "ecommerce.db"
)


def validate_arguments():
    """检查是否传入SQL文件参数。"""

    if len(sys.argv) != 2:
        print(
            "使用方法：\n"
            "python sql_learning/run_sql.py SQL文件"
        )
        return False

    return True


def validate_files(sql_file):
    """检查数据库文件和SQL文件。"""

    if not DATABASE_FILE.is_file():
        raise FileNotFoundError(
            f"数据库文件不存在：{DATABASE_FILE}"
        )

    if not sql_file.is_file():
        raise FileNotFoundError(
            f"SQL文件不存在：{sql_file}"
        )

    if sql_file.stat().st_size == 0:
        raise ValueError(
            f"SQL文件为空：{sql_file}"
        )


def read_sql(sql_file):
    """读取SQL文件内容。"""

    return sql_file.read_text(
        encoding="utf-8"
    ).strip()


def execute_sql(connection, sql):
    """执行查询并输出结果。"""

    cursor = connection.execute(sql)

    if cursor.description is None:
        connection.commit()

        print("SQL执行完成")
        print(
            f"影响行数：{cursor.rowcount}"
        )
        return

    columns = [
        description[0]
        for description in cursor.description
    ]

    rows = cursor.fetchall()

    print("\n========== 查询结果 ==========")
    print(" | ".join(columns))
    print("-" * 80)

    for row in rows:
        print(
            " | ".join(
                str(value)
                if value is not None
                else "NULL"
                for value in row
            )
        )

    print("-" * 80)
    print(
        f"返回行数：{len(rows)}"
    )


def main():
    if not validate_arguments():
        sys.exit(1)

    sql_file = Path(
        sys.argv[1]
    ).resolve()

    validate_files(sql_file)

    sql = read_sql(sql_file)

    print(
        f"数据库：{DATABASE_FILE}"
    )
    print(
        f"SQL文件：{sql_file}"
    )

    try:
        with sqlite3.connect(
            DATABASE_FILE
        ) as connection:
            execute_sql(
                connection,
                sql
            )

    except sqlite3.Error as error:
        print(
            f"SQL执行失败：{error}"
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
