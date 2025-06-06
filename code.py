import pandas as pd
import glob, os

# ─── 1) PATTERN CONFIGURATION (relative to this script’s folder) ────────────────

DEAL_PATTERN      = "deals_PitchBook_Search_Result_Columns_*.xlsx"
INVESTOR_PATTERN  = "investors_PitchBook_Search_Result_Columns_*.xlsx"
COMPANY_PATTERN   = "companies_PitchBook_Search_Result_Columns_*.xlsx"
MAPPING_PATTERN   = "PitchBook_Deal_Investors_*.xlsx"

OUTPUT_EXCEL = "final_merged_deals_companies_investors.xlsx"
# If you prefer CSV, uncomment the next line and comment out the .to_excel(...) at the end:
# OUTPUT_CSV   = "final_merged_deals_companies_investors.csv"

# ─── 2) HELPER: load & prefix each group of files (deals / investors / companies) ─────

def load_and_prefix(glob_pattern: str, prefix: str, file_type: str, header_row: int) -> pd.DataFrame:
    """
    1. Find all files matching `glob_pattern` (in current directory).
    2. Read each with pandas.read_excel(header=header_row).
    3. Add two new columns, BUT renamed as:
         prefix + "file_source" and prefix + "file_source_type"
       so they never clash in the final merge.
    4. Rename every *other* column → prefix + original_column_name.
    5. Concatenate all DataFrames into one and return it.
    """
    file_list = glob.glob(glob_pattern)
    if not file_list:
        raise FileNotFoundError(f"No files matched: {glob_pattern}")

    df_list = []
    for path in file_list:
        df = pd.read_excel(path, header=header_row)

        # (a) Add “file_source” + “file_source_type” under the prefix
        df[f"{prefix}file_source"]      = os.path.basename(path)
        df[f"{prefix}file_source_type"] = file_type

        # (b) Rename every *other* column → prefix + original name
        df = df.rename(
            columns=lambda c: f"{prefix}{c}"
                              if c not in [f"{prefix}file_source", f"{prefix}file_source_type"]
                              else c
        )
        df_list.append(df)

    combined = pd.concat(df_list, ignore_index=True)
    return combined

# ─── 3) LOAD & DEDUPE “deals”, “investors”, “companies” ──────────────────────────

# 3.1. DEALS (true header row = Excel row 9 → pandas header=8)
deals_union = load_and_prefix(
    glob_pattern=DEAL_PATTERN,
    prefix="deals.",
    file_type="deals",
    header_row=8
)
# Drop exact‐row duplicates in deals_union
deals_union = deals_union.drop_duplicates(keep="first", ignore_index=True)

# 3.2. INVESTORS (header=8)
investors_union = load_and_prefix(
    glob_pattern=INVESTOR_PATTERN,
    prefix="inv.",
    file_type="investors",
    header_row=8
)
investors_union = investors_union.drop_duplicates(keep="first", ignore_index=True)

# 3.3. COMPANIES (header=8)
companies_union = load_and_prefix(
    glob_pattern=COMPANY_PATTERN,
    prefix="comp.",
    file_type="companies",
    header_row=8
)
companies_union = companies_union.drop_duplicates(keep="first", ignore_index=True)


# ─── 4) LOAD & TRIM THE MAPPING FILE “PitchBook_Deal_Investors…” ──────────────────

mapping_files = glob.glob(MAPPING_PATTERN)
if not mapping_files:
    raise FileNotFoundError(f"No files matched: {MAPPING_PATTERN}")

mapping_path = mapping_files[0]

# The mapping’s true column headers are on Excel‐row 7 → header=6:
mapping_df = pd.read_excel(mapping_path, header=6)

# Keep ONLY the three ID columns. Discard everything else from mapping_df.
mapping_df = mapping_df[["Deal ID", "Company ID", "Investor ID"]]

# Rename them to “map.”‐prefixed columns so we don’t clash.
mapping_df = mapping_df.rename(
    columns={
        "Deal ID":     "map.Deal ID",
        "Company ID":  "map.Company ID",
        "Investor ID": "map.Investor ID"
    }
)

# Drop any duplicate (Deal, Company, Investor) triples:
mapping_df = mapping_df.drop_duplicates(
    subset=["map.Deal ID", "map.Company ID", "map.Investor ID"],
    keep="first",
    ignore_index=True
)


# ─── 5) MERGE SEQUENCE ─────────────────────────────────────────────────────────────

# 5.a) Merge mapping ↔ deals_union  on  map.Deal ID  =  deals.Deal ID
merged_step1 = pd.merge(
    mapping_df,
    deals_union,
    left_on  = "map.Deal ID",
    right_on = "deals.Deal ID",
    how      = "left",
)

# 5.b) Merge that ↔ investors_union  on  map.Investor ID  =  inv.Investor ID
merged_step2 = pd.merge(
    merged_step1,
    investors_union,
    left_on  = "map.Investor ID",
    right_on = "inv.Investor ID",
    how      = "left",
)

# 5.c) Merge that ↔ companies_union   on  map.Company ID   =  comp.Company ID
merged_full = pd.merge(
    merged_step2,
    companies_union,
    left_on  = "map.Company ID",
    right_on = "comp.Company ID",
    how      = "left",
)


# ─── 6) CREATE “deals.Investor ID” & DROP TEMP “map.” COLUMNS ─────────────────────

# We want the first three columns to read:
#   deals.Deal ID  |  deals.Company ID  |  deals.Investor ID
# Even though “Investor ID” came from mapping, we copy it into a
# column named exactly “deals.Investor ID”:
merged_full["deals.Investor ID"] = merged_full["map.Investor ID"]

# Now drop the three “map.” columns so they do NOT appear in the final:
merged_full = merged_full.drop(
    columns=["map.Deal ID", "map.Company ID", "map.Investor ID"]
)


# ─── 7) DROP ANY FINAL DUPLICATES BASED ON THE KEY TRIPLE ─────────────────────────

merged_full = merged_full.drop_duplicates(
    subset=["deals.Deal ID", "deals.Investor ID", "deals.Company ID"],
    keep="first",
    ignore_index=True
)


# ─── 8) REORDER COLUMNS ────────────────────────────────────────────────────────────

all_cols = list(merged_full.columns)

# The professor wants the first three columns to be EXACTLY these:
desired_front = ["deals.Deal ID", "deals.Company ID", "deals.Investor ID"]

# Now gather all remaining “deals.” columns (except those three)
remaining_deals = [
    c for c in all_cols
    if c.startswith("deals.") and c not in desired_front
]

# Then all “inv.” columns
remaining_inv = [c for c in all_cols if c.startswith("inv.")]

# Then all “comp.” columns
remaining_comp = [c for c in all_cols if c.startswith("comp.")]

# Finally, collect the six “file_source” / “file_source_type” columns:
#   - deals.file_source       | deals.file_source_type
#   - inv.file_source         | inv.file_source_type
#   - comp.file_source        | comp.file_source_type
file_cols = [
    "deals.file_source", "deals.file_source_type",
    "inv.file_source",   "inv.file_source_type",
    "comp.file_source",  "comp.file_source_type"
]
# Only keep those that actually exist:
file_cols = [c for c in file_cols if c in merged_full.columns]

# Build the final order:
final_order = (
    desired_front
    + remaining_deals
    + remaining_inv
    + remaining_comp
    + file_cols
)

merged_full = merged_full[final_order]


# ─── 9) SAVE THE FINAL OUTPUT ─────────────────────────────────────────────────────

# (a) Write out as Excel
merged_full.to_excel(OUTPUT_EXCEL, index=False)
print(f"✅ Final merged (and deduplicated) Excel written to: {OUTPUT_EXCEL}")

# (b) If you’d rather have CSV, uncomment the next two lines instead:
# merged_full.to_csv(OUTPUT_CSV, index=False)
# print(f"✅ Final merged (and deduplicated) CSV written to: {OUTPUT_CSV}")


# ─── 10) (OPTIONAL) HOW TO ADD A “PEOPLE” FILE ─────────────────────────────────────
# If your professor later gives you “People.xlsx” or similar (containing
# first/last name, email, etc. for each Investor ID), you can union join and
# merge it just like we did above. For example:
#
#   PEOPLE_PATTERN = "people_*.xlsx"
#   people_union = load_and_prefix(
#       glob_pattern=PEOPLE_PATTERN,
#       prefix="people.",
#       file_type="people",
#       header_row=HEADER_ROW_FOR_PEOPLE
#   )
#   # (Maybe drop duplicates in people_union by “people.Investor ID”)
#   merged_with_people = pd.merge(
#       merged_full,
#       people_union,
#       left_on="deals.Investor ID",      # or “map.Investor ID” if you hadn’t dropped that
#       right_on="people.Investor ID",
#       how="left"
#   )
#   # Then reorder columns again as needed, and save that new merged_with_people…
#
# But since you only asked for deals/investors/companies right now, the above
# block can remain commented out until you actually have a People file.

