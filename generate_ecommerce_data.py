"""Generate coherent e-commerce data plus a controlled dirty-data copy.

Outputs (under --output-dir, default: data):
  raw/order.csv, raw/order_detail.csv, raw/customer.csv, raw/product.csv
  dirty/order.csv, dirty/order_detail.csv, dirty/customer.csv, dirty/product.csv
  dirty/quality_injection_manifest.csv

The raw dataset is suitable for running an end-to-end ETL project.  The dirty
dataset starts from the same coherent facts, then receives small, documented
anomalies for data-quality and governance exercises.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd


PROVINCES = [
    ("110000", "北京市"), ("120000", "天津市"), ("130000", "河北省"),
    ("140000", "山西省"), ("150000", "内蒙古自治区"), ("210000", "辽宁省"),
    ("220000", "吉林省"), ("230000", "黑龙江省"), ("310000", "上海市"),
    ("320000", "江苏省"), ("330000", "浙江省"), ("340000", "安徽省"),
    ("350000", "福建省"), ("360000", "江西省"), ("370000", "山东省"),
    ("410000", "河南省"), ("420000", "湖北省"), ("430000", "湖南省"),
    ("440000", "广东省"), ("450000", "广西壮族自治区"), ("460000", "海南省"),
    ("500000", "重庆市"), ("510000", "四川省"), ("520000", "贵州省"),
    ("530000", "云南省"), ("540000", "西藏自治区"), ("610000", "陕西省"),
    ("620000", "甘肃省"), ("630000", "青海省"), ("640000", "宁夏回族自治区"),
    ("650000", "新疆维吾尔自治区"),
]

CATEGORIES = [
    ("CAT001", "手机数码"), ("CAT002", "电脑办公"), ("CAT003", "家用电器"),
    ("CAT004", "服装鞋包"), ("CAT005", "食品饮料"), ("CAT006", "美妆个护"),
    ("CAT007", "家居家装"), ("CAT008", "母婴用品"), ("CAT009", "运动户外"),
    ("CAT010", "图书文娱"), ("CAT011", "汽车用品"), ("CAT012", "医药保健"),
]


@dataclass(frozen=True)
class GeneratorConfig:
    orders: int = 100_000
    customers: int = 10_000
    products: int = 3_000
    shops: int = 500
    seed: int = 20260720
    start_date: str = "2025-01-01"
    end_date: str = "2025-12-31"
    dirty_rate: float = 0.001


def sequential_ids(prefix: str, count: int, width: int) -> np.ndarray:
    return np.array([f"{prefix}{number:0{width}d}" for number in range(1, count + 1)])


def random_datetimes(
    rng: np.random.Generator,
    start: str,
    end: str,
    size: int,
) -> pd.DatetimeIndex:
    start_value = pd.Timestamp(start).value // 10**9
    end_value = pd.Timestamp(end).value // 10**9
    seconds = rng.integers(start_value, end_value + 1, size=size)
    return pd.to_datetime(seconds, unit="s")


def build_customers(config: GeneratorConfig, rng: np.random.Generator) -> pd.DataFrame:
    customer_ids = sequential_ids("C", config.customers, 6)
    province_index = rng.integers(0, len(PROVINCES), size=config.customers)
    register_time = random_datetimes(
        rng, "2022-01-01", "2024-12-31 23:59:59", config.customers
    )
    updated_at = register_time + pd.to_timedelta(
        rng.integers(0, 366, size=config.customers), unit="D"
    )

    return pd.DataFrame(
        {
            "customer_id": customer_ids,
            "customer_name": [f"客户{number:06d}" for number in range(1, config.customers + 1)],
            "gender": rng.choice(["男", "女", "未知"], size=config.customers, p=[0.49, 0.49, 0.02]),
            "age": rng.integers(18, 71, size=config.customers),
            "phone": [f"1{number:010d}" for number in range(1, config.customers + 1)],
            "email": [f"customer{number:06d}@example.com" for number in range(1, config.customers + 1)],
            "register_time": register_time,
            "member_level": rng.choice(
                ["普通", "银卡", "金卡", "钻石"], size=config.customers, p=[0.70, 0.18, 0.09, 0.03]
            ),
            "province_code": [PROVINCES[index][0] for index in province_index],
            "province_name": [PROVINCES[index][1] for index in province_index],
            "customer_status": rng.choice(["正常", "冻结"], size=config.customers, p=[0.99, 0.01]),
            "updated_at": updated_at,
        }
    )


def build_products(config: GeneratorConfig, rng: np.random.Generator) -> pd.DataFrame:
    product_ids = sequential_ids("P", config.products, 6)
    category_index = rng.integers(0, len(CATEGORIES), size=config.products)
    unit_price = np.round(rng.uniform(10, 10_000, size=config.products), 2)
    cost_price = np.round(unit_price * rng.uniform(0.50, 0.85, size=config.products), 2)
    created_at = random_datetimes(
        rng, "2022-01-01", "2024-12-31 23:59:59", config.products
    )
    updated_at = created_at + pd.to_timedelta(
        rng.integers(0, 366, size=config.products), unit="D"
    )

    return pd.DataFrame(
        {
            "product_id": product_ids,
            "product_name": [f"商品{number:06d}" for number in range(1, config.products + 1)],
            "category_id": [CATEGORIES[index][0] for index in category_index],
            "category_name": [CATEGORIES[index][1] for index in category_index],
            "brand_name": [f"品牌{number:03d}" for number in rng.integers(1, 201, size=config.products)],
            "unit_price": unit_price,
            "cost_price": cost_price,
            "product_status": rng.choice(["在售", "下架"], size=config.products, p=[0.97, 0.03]),
            "created_at": created_at,
            "updated_at": updated_at,
        }
    )


def build_orders_and_details(
    config: GeneratorConfig,
    rng: np.random.Generator,
    customers: pd.DataFrame,
    products: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    order_ids = sequential_ids("O", config.orders, 8)
    customer_ids = rng.choice(customers["customer_id"].to_numpy(), size=config.orders)
    order_time = random_datetimes(rng, config.start_date, config.end_date, config.orders)
    details_per_order = rng.integers(1, 6, size=config.orders)
    detail_count = int(details_per_order.sum())

    detail_order_ids = np.repeat(order_ids, details_per_order)
    detail_order_times = pd.DatetimeIndex(np.repeat(order_time.to_numpy(), details_per_order))
    product_index = rng.integers(0, config.products, size=detail_count)
    product_ids = products["product_id"].to_numpy()[product_index]
    unit_prices = products["unit_price"].to_numpy()[product_index]
    quantities = rng.integers(1, 6, size=detail_count)
    gross_amount = np.round(unit_prices * quantities, 2)
    selected_discount = rng.choice(
        np.array([0.0, 5.0, 10.0, 20.0, 50.0]),
        size=detail_count,
        p=[0.55, 0.12, 0.13, 0.12, 0.08],
    )
    discount_amount = np.round(np.minimum(selected_discount, gross_amount * 0.30), 2)
    actual_amount = np.round(gross_amount - discount_amount, 2)
    created_at = detail_order_times + pd.to_timedelta(
        rng.integers(0, 3_601, size=detail_count), unit="s"
    )

    details = pd.DataFrame(
        {
            "order_detail_id": sequential_ids("OD", detail_count, 9),
            "order_id": detail_order_ids,
            "product_id": product_ids,
            "quantity": quantities,
            "unit_price": unit_prices,
            "discount_amount": discount_amount,
            "actual_amount": actual_amount,
            "created_at": created_at,
        }
    )

    # The order amount is derived from its details; it is never generated independently.
    order_amount = (
        details.groupby("order_id", sort=False)["actual_amount"]
        .sum()
        .reindex(order_ids)
        .to_numpy()
    )
    coupon_candidate = rng.choice(
        np.array([0.0, 10.0, 20.0, 50.0, 100.0, 200.0]),
        size=config.orders,
        p=[0.50, 0.12, 0.12, 0.12, 0.09, 0.05],
    )
    coupon_amount = np.round(np.minimum(coupon_candidate, order_amount * 0.20), 2)
    freight_amount = rng.choice(
        np.array([0.0, 6.0, 8.0, 12.0]), size=config.orders, p=[0.55, 0.15, 0.20, 0.10]
    )
    payable_amount = np.round(order_amount - coupon_amount + freight_amount, 2)
    province_index = rng.integers(0, len(PROVINCES), size=config.orders)
    updated_at = order_time + pd.to_timedelta(
        rng.integers(0, 31, size=config.orders), unit="D"
    )

    orders = pd.DataFrame(
        {
            "order_id": order_ids,
            "order_no": [f"NO{timestamp:%Y%m%d}{number:08d}" for number, timestamp in enumerate(order_time, 1)],
            "customer_id": customer_ids,
            "shop_id": [f"S{number:05d}" for number in rng.integers(1, config.shops + 1, size=config.orders)],
            "order_time": order_time,
            "order_status": rng.choice(
                ["待支付", "已支付", "已发货", "已完成", "已取消", "已退款"],
                size=config.orders,
                p=[0.03, 0.12, 0.18, 0.55, 0.08, 0.04],
            ),
            "order_amount": np.round(order_amount, 2),
            "coupon_amount": coupon_amount,
            "freight_amount": freight_amount,
            "payable_amount": payable_amount,
            "order_channel": rng.choice(["APP", "小程序", "PC", "线下"], size=config.orders),
            "receiver_province_code": [PROVINCES[index][0] for index in province_index],
            "receiver_province_name": [PROVINCES[index][1] for index in province_index],
            "updated_at": updated_at,
        }
    )

    return orders, details


def sample_indices(rng: np.random.Generator, size: int, rate: float) -> np.ndarray:
    count = max(1, int(round(size * rate)))
    return rng.choice(size, size=min(count, size), replace=False)


def inject_dirty_data(
    config: GeneratorConfig,
    rng: np.random.Generator,
    customers: pd.DataFrame,
    products: pd.DataFrame,
    orders: pd.DataFrame,
    details: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    dirty_customers = customers.copy(deep=True)
    dirty_products = products.copy(deep=True)
    dirty_orders = orders.copy(deep=True)
    dirty_details = details.copy(deep=True)
    manifest: list[dict[str, object]] = []

    def record(dataset: str, row_id: object, rule: str, field: str, value: object, note: str) -> None:
        manifest.append(
            {
                "dataset": dataset,
                "row_id": row_id,
                "rule_code": rule,
                "field": field,
                "injected_value": value,
                "description": note,
            }
        )

    # Quantity range errors.  Amount is deliberately not recalculated so a business rule also catches them.
    indices = sample_indices(rng, len(dirty_details), config.dirty_rate)
    split = len(indices) // 2
    dirty_details.loc[indices[:split], "quantity"] = -1
    dirty_details.loc[indices[split:], "quantity"] = 999
    for index in indices:
        row = dirty_details.loc[index]
        record("order_detail", row["order_detail_id"], "QUANTITY_RANGE", "quantity", row["quantity"], "商品数量超出1至5")

    # Detail amount formula errors.
    indices = sample_indices(rng, len(dirty_details), config.dirty_rate)
    dirty_details.loc[indices, "actual_amount"] = np.round(
        dirty_details.loc[indices, "actual_amount"] * 1.20 + 1, 2
    )
    for index in indices:
        row = dirty_details.loc[index]
        record("order_detail", row["order_detail_id"], "DETAIL_AMOUNT_FORMULA", "actual_amount", row["actual_amount"], "明细金额不等于数量乘单价减优惠")

    # Missing product master references.
    indices = sample_indices(rng, len(dirty_details), config.dirty_rate)
    dirty_details.loc[indices, "product_id"] = [f"P_MISSING_{number:06d}" for number in range(1, len(indices) + 1)]
    for index in indices:
        row = dirty_details.loc[index]
        record("order_detail", row["order_detail_id"], "PRODUCT_FK", "product_id", row["product_id"], "商品主数据不存在")

    # Detail timestamps before their orders.
    indices = sample_indices(rng, len(dirty_details), config.dirty_rate)
    order_time_lookup = orders.set_index("order_id")["order_time"]
    dirty_details.loc[indices, "created_at"] = (
        dirty_details.loc[indices, "order_id"].map(order_time_lookup)
        - pd.to_timedelta(rng.integers(1, 31, size=len(indices)), unit="D")
    )
    for index in indices:
        row = dirty_details.loc[index]
        record("order_detail", row["order_detail_id"], "DETAIL_TIME", "created_at", row["created_at"], "明细时间早于订单时间")

    # Missing customer master references.
    indices = sample_indices(rng, len(dirty_orders), config.dirty_rate)
    dirty_orders.loc[indices, "customer_id"] = [f"C_MISSING_{number:06d}" for number in range(1, len(indices) + 1)]
    for index in indices:
        row = dirty_orders.loc[index]
        record("order", row["order_id"], "CUSTOMER_FK", "customer_id", row["customer_id"], "客户主数据不存在")

    # Payable amount formula errors, including zero and negative values.
    indices = sample_indices(rng, len(dirty_orders), config.dirty_rate)
    thirds = np.array_split(indices, 3)
    dirty_orders.loc[thirds[0], "payable_amount"] = 0.0
    dirty_orders.loc[thirds[1], "payable_amount"] = -1.0
    dirty_orders.loc[thirds[2], "payable_amount"] = np.round(
        dirty_orders.loc[thirds[2], "payable_amount"] + 123.45, 2
    )
    for index in indices:
        row = dirty_orders.loc[index]
        record("order", row["order_id"], "ORDER_AMOUNT_FORMULA", "payable_amount", row["payable_amount"], "应付金额公式异常")

    # Invalid enum and missing non-key attributes.
    indices = sample_indices(rng, len(dirty_orders), config.dirty_rate)
    dirty_orders.loc[indices, "order_status"] = "未知状态"
    for index in indices:
        row = dirty_orders.loc[index]
        record("order", row["order_id"], "ORDER_STATUS_ENUM", "order_status", row["order_status"], "订单状态不在标准枚举中")

    indices = sample_indices(rng, len(dirty_customers), config.dirty_rate)
    dirty_customers.loc[indices, "customer_name"] = pd.NA
    for index in indices:
        row = dirty_customers.loc[index]
        record("customer", row["customer_id"], "CUSTOMER_NAME_NULL", "customer_name", None, "客户名称为空")

    indices = sample_indices(rng, len(dirty_products), config.dirty_rate)
    dirty_products.loc[indices, "category_name"] = pd.NA
    for index in indices:
        row = dirty_products.loc[index]
        record("product", row["product_id"], "CATEGORY_NULL", "category_name", None, "商品分类为空")

    # Exact duplicates are appended last so duplicate rules have known expected rows.
    order_indices = sample_indices(rng, len(dirty_orders), config.dirty_rate / 2)
    duplicate_orders = dirty_orders.loc[order_indices].copy()
    dirty_orders = pd.concat([dirty_orders, duplicate_orders], ignore_index=True)
    for _, row in duplicate_orders.iterrows():
        record("order", row["order_id"], "DUPLICATE_ROW", "all", "duplicate", "订单整行重复")

    detail_indices = sample_indices(rng, len(dirty_details), config.dirty_rate / 2)
    duplicate_details = dirty_details.loc[detail_indices].copy()
    dirty_details = pd.concat([dirty_details, duplicate_details], ignore_index=True)
    for _, row in duplicate_details.iterrows():
        record("order_detail", row["order_detail_id"], "DUPLICATE_ROW", "all", "duplicate", "订单明细整行重复")

    manifest_df = pd.DataFrame(manifest)
    return dirty_customers, dirty_products, dirty_orders, dirty_details, manifest_df


def validate_clean_data(
    customers: pd.DataFrame,
    products: pd.DataFrame,
    orders: pd.DataFrame,
    details: pd.DataFrame,
) -> None:
    errors: list[str] = []

    for name, frame, key in [
        ("customer", customers, "customer_id"),
        ("product", products, "product_id"),
        ("order", orders, "order_id"),
        ("order_detail", details, "order_detail_id"),
    ]:
        if frame[key].isna().any() or frame[key].duplicated().any():
            errors.append(f"{name}.{key} is null or duplicated")

    if not orders["customer_id"].isin(customers["customer_id"]).all():
        errors.append("order contains unknown customer_id")
    if not details["order_id"].isin(orders["order_id"]).all():
        errors.append("order_detail contains unknown order_id")
    if not details["product_id"].isin(products["product_id"]).all():
        errors.append("order_detail contains unknown product_id")
    if not details["quantity"].between(1, 5).all():
        errors.append("clean quantity is outside 1..5")

    expected_detail_amount = np.round(
        details["quantity"] * details["unit_price"] - details["discount_amount"], 2
    )
    if not np.allclose(details["actual_amount"], expected_detail_amount, atol=0.01):
        errors.append("actual_amount formula mismatch")

    detail_sum = details.groupby("order_id")["actual_amount"].sum()
    order_amount = orders.set_index("order_id")["order_amount"]
    if not np.allclose(order_amount.sort_index(), detail_sum.sort_index(), atol=0.01):
        errors.append("order_amount does not equal detail actual_amount sum")

    expected_payable = np.round(
        orders["order_amount"] - orders["coupon_amount"] + orders["freight_amount"], 2
    )
    if not np.allclose(orders["payable_amount"], expected_payable, atol=0.01):
        errors.append("payable_amount formula mismatch")

    order_time_lookup = orders.set_index("order_id")["order_time"]
    detail_order_time = details["order_id"].map(order_time_lookup)
    if (details["created_at"] < detail_order_time).any():
        errors.append("detail created_at is before order_time")

    if errors:
        raise ValueError("Clean data validation failed: " + "; ".join(errors))


def write_dataset(directory: Path, frames: dict[str, pd.DataFrame]) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    for filename, frame in frames.items():
        frame.to_csv(directory / filename, index=False, encoding="utf-8-sig")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate coherent e-commerce learning data")
    parser.add_argument("--orders", type=int, default=100_000)
    parser.add_argument("--customers", type=int, default=10_000)
    parser.add_argument("--products", type=int, default=3_000)
    parser.add_argument("--shops", type=int, default=500)
    parser.add_argument("--seed", type=int, default=20260720)
    parser.add_argument("--dirty-rate", type=float, default=0.001)
    parser.add_argument("--output-dir", type=Path, default=Path("../data"))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.orders < 1 or args.customers < 1 or args.products < 1 or args.shops < 1:
        raise ValueError("orders/customers/products/shops must all be positive")
    if not 0 < args.dirty_rate <= 0.05:
        raise ValueError("dirty-rate must be greater than 0 and no more than 0.05")

    config = GeneratorConfig(
        orders=args.orders,
        customers=args.customers,
        products=args.products,
        shops=args.shops,
        seed=args.seed,
        dirty_rate=args.dirty_rate,
    )
    rng = np.random.default_rng(config.seed)

    customers = build_customers(config, rng)
    products = build_products(config, rng)
    orders, details = build_orders_and_details(config, rng, customers, products)
    validate_clean_data(customers, products, orders, details)

    dirty_customers, dirty_products, dirty_orders, dirty_details, manifest = inject_dirty_data(
        config, rng, customers, products, orders, details
    )

    clean_frames = {
        "customer.csv": customers,
        "product.csv": products,
        "order.csv": orders,
        "order_detail.csv": details,
    }
    dirty_frames = {
        "customer.csv": dirty_customers,
        "product.csv": dirty_products,
        "order.csv": dirty_orders,
        "order_detail.csv": dirty_details,
        "quality_injection_manifest.csv": manifest,
    }
    write_dataset(args.output_dir / "raw", clean_frames)
    write_dataset(args.output_dir / "dirty", dirty_frames)

    print("E-commerce data generation completed")
    print(f"Clean output: {(args.output_dir / 'raw').resolve()}")
    print(f"Dirty output: {(args.output_dir / 'dirty').resolve()}")
    print(f"Customers: {len(customers):,}")
    print(f"Products: {len(products):,}")
    print(f"Orders: {len(orders):,}")
    print(f"Order details: {len(details):,}")
    print(f"Injected quality issues: {len(manifest):,}")


if __name__ == "__main__":
    main()
