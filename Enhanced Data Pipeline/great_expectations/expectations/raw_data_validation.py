import great_expectations as ge
import pandas as pd


def validate_csv(path_to_csv):
    """
    Validate the CSV file using Great Expectations:
      1) 'order_id' must not be null
      2) 'amount' must be > 0
      3) 'customer_id' must be >= 1
    """
    df = pd.read_csv(path_to_csv)
    ge_df = ge.from_pandas(df)

    # Expect order_id to not be null
    result_order_id = ge_df.expect_column_values_to_not_be_null('order_id')
    if not result_order_id.success:
        raise ValueError("Validation failed: Found null values in 'order_id'")

    # Expect amount to be > 0
    result_amount = ge_df.expect_column_values_to_be_between('amount', 0.01, 9999999, strictly=True)
    if not result_amount.success:
        raise ValueError("Validation failed: 'amount' must be > 0")

    # Expect customer_id to be >= 1
    result_customer = ge_df.expect_column_values_to_be_between('customer_id', 1, 9999999, strictly=True)
    if not result_customer.success:
        raise ValueError("Validation failed: 'customer_id' must be >= 1")

    print("Validation passed for raw_data_validation.")


if __name__ == "__main__":
    validate_csv("/tmp/orders.csv")
