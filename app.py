from fastapi import FastAPI, Query
from pydantic import BaseModel
import pandas as pd
import base64
import io

app = FastAPI()

class ValidateRequest(BaseModel):
    issue_file_base64: str
    received_file_base64: str
    route_card: str = None
    supplier: str = None

@app.post("/validate")
async def validate(request: ValidateRequest):

    # Decode Base64 to Excel files
    issue_bytes = base64.b64decode(request.issue_file_base64)
    received_bytes = base64.b64decode(request.received_file_base64)

    # Read into DataFrames
    issue_df = pd.read_excel(io.BytesIO(issue_bytes))
    received_df = pd.read_excel(io.BytesIO(received_bytes))

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

    # STEP 1: Aggregate ISSUE
    issue_grouped = (
        issue_df
        .groupby(["RouteCard No", "GK DC No", "FG Item Code", "Supplier Name"], dropna=False)["Transfer Qty"]
        .sum()
        .reset_index()
        .rename(columns={"GK DC No": "DC No", "Transfer Qty": "Issue_Qty"})
    )

    # STEP 2: Aggregate RECEIVED
    received_grouped = (
        received_df
        .groupby(["RouteCard No", "Subcon DC No", "FG Item Code", "Supplier Name"], dropna=False)["Rcvd. Qty"]
        .sum()
        .reset_index()
        .rename(columns={"Subcon DC No": "DC No", "Rcvd. Qty": "Received_Qty"})
    )

    # STEP 3: Merge
    merged = issue_grouped.merge(
        received_grouped,
        on=["RouteCard No", "DC No", "FG Item Code", "Supplier Name"],
        how="outer"
    )

    merged["Issue_Qty"] = merged["Issue_Qty"].fillna(0)
    merged["Received_Qty"] = merged["Received_Qty"].fillna(0)

    # STEP 4: Compute Difference
    merged["Difference"] = merged["Issue_Qty"] - merged["Received_Qty"]
    mismatch_df = merged[merged["Difference"] != 0]

    # STEP 5: Optional Filters
    if request.route_card:
        mismatch_df = mismatch_df[mismatch_df["RouteCard No"].astype(str) == str(request.route_card)]

    if request.supplier:
        mismatch_df = mismatch_df[mismatch_df["Supplier Name"] == request.supplier]

    mismatch_df = mismatch_df.fillna("")

    return {
        "mismatch_count": len(mismatch_df),
        "mismatch_preview": mismatch_df.head(100).to_dict(orient="records")
    }