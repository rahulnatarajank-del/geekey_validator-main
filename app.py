from fastapi import FastAPI, UploadFile, File, Query
import pandas as pd

app = FastAPI()

@app.post("/validate")
async def validate(
    issue_file: UploadFile = File(...),
    received_file: UploadFile = File(...),
    route_card: str = Query(None),
    supplier: str = Query(None)
):

    # Read files
    issue_df = pd.read_excel(issue_file.file)
    received_df = pd.read_excel(received_file.file)

    # Normalize key columns to string
    issue_df["RouteCard No"] = issue_df["RouteCard No"].astype(str).str.strip()
    received_df["RouteCard No"] = received_df["RouteCard No"].astype(str).str.strip()

    issue_df["GK DC No"] = issue_df["GK DC No"].astype(str).str.strip()
    received_df["Subcon DC No"] = received_df["Subcon DC No"].astype(str).str.strip()

    issue_df["FG Item Code"] = issue_df["FG Item Code"].astype(str).str.strip()
    received_df["FG Item Code"] = received_df["FG Item Code"].astype(str).str.strip()

    issue_df["Supplier Name"] = issue_df["Supplier Name"].astype(str).str.strip()
    received_df["Supplier Name"] = received_df["Supplier Name"].astype(str).str.strip()


    # Clean column spaces
    issue_df.columns = issue_df.columns.str.strip()
    received_df.columns = received_df.columns.str.strip()

    # -----------------------------
    # STEP 1: Aggregate ISSUE
    # -----------------------------
    issue_grouped = (
        issue_df
        .groupby(
            ["RouteCard No", "GK DC No", "FG Item Code", "Supplier Name"],
            dropna=False
        )["Transfer Qty"]
        .sum()
        .reset_index()
        .rename(columns={
            "GK DC No": "DC No",
            "Transfer Qty": "Issue_Qty"
        })
    )

    # -----------------------------
    # STEP 2: Aggregate RECEIVED
    # -----------------------------
    received_grouped = (
        received_df
        .groupby(
            ["RouteCard No", "Subcon DC No", "FG Item Code", "Supplier Name"],
            dropna=False
        )["Rcvd. Qty"]
        .sum()
        .reset_index()
        .rename(columns={
            "Subcon DC No": "DC No",
            "Rcvd. Qty": "Received_Qty"
        })
    )

    # -----------------------------
    # STEP 3: Merge Properly
    # -----------------------------
    merged = issue_grouped.merge(
        received_grouped,
        on=["RouteCard No", "DC No", "FG Item Code", "Supplier Name"],
        how="outer"
    )

    # Replace NaN with 0 for quantity comparison
    merged["Issue_Qty"] = merged["Issue_Qty"].fillna(0)
    merged["Received_Qty"] = merged["Received_Qty"].fillna(0)

    # -----------------------------
    # STEP 4: Compute Difference
    # -----------------------------
    merged["Difference"] = merged["Issue_Qty"] - merged["Received_Qty"]

    # Only mismatches
    mismatch_df = merged[merged["Difference"] != 0]

    # -----------------------------
    # STEP 5: Optional Filters
    # -----------------------------
    if route_card:
        mismatch_df = mismatch_df[
            mismatch_df["RouteCard No"].astype(str) == str(route_card)
        ]

    if supplier:
        mismatch_df = mismatch_df[
            mismatch_df["Supplier Name"] == supplier
        ]

    # Ensure JSON safe (no NaN)
    mismatch_df = mismatch_df.fillna("")

    return {
        "mismatch_count": len(mismatch_df),
        "mismatch_preview": mismatch_df.head(100).to_dict(orient="records")
    }
