import pandas as pd

from warehouse.base_builder import BaseBuilder

def test_base_builder():
    builder = BaseBuilder()

    test_df = pd.DataFrame({
        "customer_id": ["C001", "C002"],
        "customer_name": ["张三", "李四"],
        "total_amount": [100.50, 200.80]
    })
    output_file = "output/test/base_builder_test.csv"

    builder.save(
        test_df,
        output_file,
    )
    result_df = builder.read(
        output_file,
    )
    print("读取结果")
    print(result_df)

    print("\n字段类型：")
    print(result_df.dtypes)

    print("\n行数：", len(result_df))

    assert len(result_df) == 2
    assert list(result_df.columns) == [
        "customer_id",
        "customer_name",
        "total_amount"
    ]

    print("\nBaseBuilder测试通过")


if __name__ == "__main__":
    test_base_builder()
