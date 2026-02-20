from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import pandas as pd
import requests
from io import BytesIO

app = FastAPI(title="Subcontract Reconciliation API")


# -----------------------------
# Request Model (Azure Agent Compatible)
# -----------------------------
class ValidationRequest(BaseModel):
    issue_blob_url: str
    received_blob_url: str
    route_card: str | None = None
    supplier: str | None = None


# -----------------------------
# Validation Endpoint
# -----------------------------
@app.post("/validate")
async def validate(request: ValidationRequest):

    # -----------------------------
    # STEP 1: Download Excel Files
    # -----------------------------
    try:
        issue_response = requests.get(request.issue_blob_url)
        received_response = requests.get(request.received_blob_url)

        if issue_response.status_code != 200:
            raise HTTPException(status_code=400, detail="Failed to download issue file")

        if received_response.status_code != 200:
            raise HTTPException(status_code=400, detail="Failed to download received file")

        issue_df = pd.read_excel(BytesIO(issue_response.content))
        received_df = pd.read_excel(BytesIO(received_response.content))

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"File processing error: {str(e)}")

    route_card = request.route_card
    supplier = request.supplier

    # -----------------------------
    # STEP 2: Clean Column Names
    # -----------------------------
    issue_df.columns = issue_df.columns.str.strip()
    received_df.columns = received_df.columns.str.strip()

    # -----------------------------
    # STEP 3: Normalize Key Columns (CRITICAL)
    # -----------------------------
    issue_df["RouteCard No"] = issue_df["RouteCard No"].astype(str).str.strip()
    received_df["RouteCard No"] = received_df["RouteCard No"].astype(str).str.strip()

    issue_df["GK DC No"] = issue_df["GK DC No"].astype(str).str.strip()
    received_df["Subcon DC No"] = received_df["Subcon DC No"].astype(str).str.strip()

    issue_df["FG Item Code"] = issue_df["FG Item Code"].astype(str).str.strip()
    received_df["FG Item Code"] = received_df["FG Item Code"].astype(str).str.strip()

    issue_df["Supplier Name"] = issue_df["Supplier Name"].astype(str).str.strip()
    received_df["Supplier Name"] = received_df["Supplier Name"].astype(str).str.strip()

    # Ensure numeric quantity columns
    issue_df["Transfer Qty"] = pd.to_numeric(issue_df["Transfer Qty"], errors="coerce").fillna(0)
    received_df["Rcvd. Qty"] = pd.to_numeric(received_df["Rcvd. Qty"], errors="coerce").fillna(0)

    # -----------------------------
    # STEP 4: Aggregate ISSUE
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
    # STEP 5: Aggregate RECEIVED
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
    # STEP 6: Merge
    # -----------------------------
    merged = issue_grouped.merge(
        received_grouped,
        on=["RouteCard No", "DC No", "FG Item Code", "Supplier Name"],
        how="outer"
    )

    merged["Issue_Qty"] = merged["Issue_Qty"].fillna(0)
    merged["Received_Qty"] = merged["Received_Qty"].fillna(0)

    # -----------------------------
    # STEP 7: Calculate Difference
    # -----------------------------
    merged["Difference"] = merged["Issue_Qty"] - merged["Received_Qty"]

    # Only mismatches
    mismatch_df = merged[merged["Difference"] != 0]

    # -----------------------------
    # STEP 8: Optional Filters
    # -----------------------------
    if route_card:
        mismatch_df = mismatch_df[
            mismatch_df["RouteCard No"] == str(route_card)
        ]

    if supplier:
        mismatch_df = mismatch_df[
            mismatch_df["Supplier Name"] == supplier
        ]

    # -----------------------------
    # STEP 9: Prepare Clean Output
    # -----------------------------
    mismatch_df = mismatch_df.fillna("")

    summary = []

    for _, row in mismatch_df.iterrows():
        summary.append({
            "route_card": row["RouteCard No"],
            "dc_no": row["DC No"],
            "fg_item_code": row["FG Item Code"],
            "supplier": row["Supplier Name"],
            "issue_qty": row["Issue_Qty"],
            "received_qty": row["Received_Qty"],
            "difference": row["Difference"]
        })

    return {
        "mismatch_count": len(mismatch_df),
        "summary": summary
    }
