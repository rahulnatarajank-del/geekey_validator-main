import pandas as pd


def validate_store(issue_df: pd.DataFrame, received_df: pd.DataFrame) -> dict:
    """
    Production-grade store validation engine.
    Returns structured output including:
    - summary metrics
    - full comparison table
    - mismatch-only table
    """

    # -------------------------------
    # 1. Data Cleaning
    # -------------------------------
    issue_df = issue_df.copy()
    received_df = received_df.copy()

    issue_df['Transfer Qty'] = pd.to_numeric(
        issue_df['Transfer Qty'], errors='coerce'
    ).fillna(0)

    received_df['Rcvd. Qty'] = pd.to_numeric(
        received_df['Rcvd. Qty'], errors='coerce'
    ).fillna(0)

    issue_df['Special Price'] = pd.to_numeric(
        issue_df.get('Special Price', 0), errors='coerce'
    ).fillna(0)

    received_df['Special Price'] = pd.to_numeric(
        received_df.get('Special Price', 0), errors='coerce'
    ).fillna(0)

    # -------------------------------
    # 2. Group Issue
    # -------------------------------
    issue_grouped = issue_df.groupby(
        ['FG Item Code', 'RouteCard No', 'GK DC No'],
        as_index=False
    ).agg({
        'Transfer Qty': 'sum',
        'Special Price': 'mean'
    })

    # -------------------------------
    # 3. Group Received
    # -------------------------------
    received_grouped = received_df.groupby(
        ['FG Item Code', 'RouteCard No', 'Subcon DC No'],
        as_index=False
    ).agg({
        'Rcvd. Qty': 'sum',
        'Special Price': 'mean'
    })

    received_grouped.rename(
        columns={'Subcon DC No': 'GK DC No'},
        inplace=True
    )

    # -------------------------------
    # 4. Merge
    # -------------------------------
    result = issue_grouped.merge(
        received_grouped,
        on=['FG Item Code', 'RouteCard No', 'GK DC No'],
        how='outer',
        suffixes=('_Issued', '_Received')
    ).fillna(0)

    # -------------------------------
    # 5. Quantity Difference
    # -------------------------------
    result['Qty Difference'] = (
        result['Transfer Qty'] - result['Rcvd. Qty']
    )

    result['Receipt Status'] = result['Qty Difference'].apply(
        lambda x: "Matched" if x == 0
        else "Over Receipt" if x < 0
        else "Under Receipt"
    )

    # -------------------------------
    # 6. Price Difference
    # -------------------------------
    result['Price Difference'] = (
        result['Special Price_Issued'] -
        result['Special Price_Received']
    )

    result['Price Status'] = result['Price Difference'].apply(
        lambda x: "Matched" if x == 0 else "Mismatch"
    )

    # -------------------------------
    # 7. Overall Status
    # -------------------------------
    result['Overall Status'] = result.apply(
        lambda row: "Matched"
        if row['Receipt Status'] == "Matched"
        and row['Price Status'] == "Matched"
        else "Mismatch",
        axis=1
    )

    # -------------------------------
    # 8. Summary Metrics
    # -------------------------------
    total_records = len(result)
    total_mismatch = len(result[result['Overall Status'] == "Mismatch"])
    total_matched = len(result[result['Overall Status'] == "Matched"])
    over_receipt = len(result[result['Receipt Status'] == "Over Receipt"])
    under_receipt = len(result[result['Receipt Status'] == "Under Receipt"])
    price_mismatch = len(result[result['Price Status'] == "Mismatch"])

    summary = {
        "total_records": int(total_records),
        "matched_records": int(total_matched),
        "mismatch_records": int(total_mismatch),
        "over_receipt_cases": int(over_receipt),
        "under_receipt_cases": int(under_receipt),
        "price_mismatch_cases": int(price_mismatch)
    }

    # -------------------------------
    # 9. Mismatch Table
    # -------------------------------
    mismatch_table = result[result['Overall Status'] == "Mismatch"]

    # Convert to JSON-safe format
    return {
        "summary": summary,
        "full_table": result.to_dict(orient="records"),
        "mismatch_table": mismatch_table.to_dict(orient="records")
    }
