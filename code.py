import pandas as pd
import os

# ─── 1) INPUT FILE DEFINITIONS ──────────────────────────────────────────────────
deal_files = [
    "deals_PitchBook_Search_Result_Columns_2024_11_21_16_55_11.xlsx",
    "deals_PitchBook_Search_Result_Columns_2024_12_12_14_00_02.xlsx"
]
company_files = [
    "companies_PitchBook_Search_Result_Columns_2024_11_21_16_55_55.xlsx",
    "companies_PitchBook_Search_Result_Columns_2024_12_12_13_56_26.xlsx"
]
investor_files = [
    "investors_PitchBook_Search_Result_Columns_2024_11_21_17_00_53.xlsx",
    "investors_PitchBook_Search_Result_Columns_2024_12_30_13_49_02.xlsx",
    "investors2_PitchBook_Search_Result_Columns_2024_11_25_19_40_00.xlsx",
    "investors3_PitchBook_All_Columns_2025_05_30_15_45_26.xlsx"
]
people_files = [
    "people_PitchBook_Search_Result_Columns_2024_11_21_17_01_48.xlsx",
    "people_PitchBook_Search_Result_Columns_2024_12_12_14_08_01.xlsx"
]
mapping_files = [
    "PitchBook_Deal_Investors_2025_05_29_23_02_47.xlsx",
    "PitchBook_Deal_Investors_2025_05_30_20_36_20.xlsx"
]

OUTPUT_MERGED  = "final_merged_deals_companies_investors_people.xlsx"
OUTPUT_REFINED = "final_refined_deals_companies_investors_people.xlsx"


# ─── 2) AUTO-DETECT HEADER ROW IN A WORKBOOK ────────────────────────────────────
def detect_header_row(path, id_cols):
    preview = pd.read_excel(path, header=None, nrows=20)
    for i, row in preview.iterrows():
        vals = {str(x).strip() for x in row.dropna().astype(str)}
        if any(col in vals for col in id_cols):
            return i
    raise ValueError(f"No header row with {id_cols} found in {path}")


# ─── 3) LOAD + PREFIX + UNION FOR A GROUP ────────────────────────────────────────
def load_group(files, prefix, ftype, id_cols):
    dfs = []
    for fn in files:
        hdr = detect_header_row(fn, id_cols)
        df  = pd.read_excel(fn, header=hdr)
        # annotate source
        df[f"{prefix}file_source"]      = os.path.basename(fn)
        df[f"{prefix}file_source_type"] = ftype
        # prefix all other column names
        df = df.rename(columns=lambda c: f"{prefix}{c}" if not c.startswith(prefix) else c)
        dfs.append(df)
    return pd.concat(dfs, ignore_index=True).drop_duplicates(ignore_index=True)

deals_df     = load_group(deal_files,     "deals.",  "deals",     ["Deal ID"])
companies_df = load_group(company_files,  "comp.",   "companies", ["Company ID"])
investors_df = load_group(investor_files, "inv.",    "investors",["Investor ID"])
people_df    = load_group(people_files,   "people.", "people",    ["Investor ID","Person ID"])


# ─── 4) LOAD + UNION MAPPING FILES ───────────────────────────────────────────────
map_dfs = []
for fn in mapping_files:
    hdr = detect_header_row(fn, ["Deal ID","Company ID","Investor ID"])
    m   = pd.read_excel(fn, header=hdr)
    # keep only the 3 key columns
    m2  = m[["Deal ID","Company ID","Investor ID"]].drop_duplicates()
    m2["map.file_source"]      = os.path.basename(fn)
    m2["map.file_source_type"] = "mapping"
    map_dfs.append(m2)

mapping_df = (
    pd.concat(map_dfs, ignore_index=True)
      .drop_duplicates()
      .rename(columns={
          "Deal ID":     "map.Deal ID",
          "Company ID":  "map.Company ID",
          "Investor ID": "map.Investor ID"
      })
)


# ─── 5) MERGE IN STEPS ───────────────────────────────────────────────────────────
m1 = mapping_df.merge(deals_df,
                      left_on="map.Deal ID",  right_on="deals.Deal ID",
                      how="left")
m2 = m1.merge(investors_df,
              left_on="map.Investor ID", right_on="inv.Investor ID",
              how="left")
m3 = m2.merge(companies_df,
              left_on="map.Company ID", right_on="comp.Company ID",
              how="left")

# pick correct people-key
for c in ("people.Investor ID","people.Person ID","people.PBId"):
    if c in people_df.columns:
        people_key = c
        break
else:
    raise KeyError("No suitable join‐key in people_df")

merged = m3.merge(people_df,
                 left_on="map.Investor ID", right_on=people_key,
                 how="left")


# ─── 6) DROP MAP COLUMNS & DEDUPE ───────────────────────────────────────────────
merged["deals.Investor ID"] = merged["map.Investor ID"]
final_df = (
    merged
      .drop(columns=["map.Deal ID","map.Company ID","map.Investor ID"])
      .drop_duplicates(
          subset=["deals.Deal ID","deals.Investor ID","deals.Company ID"],
          ignore_index=True
      )
)


# ─── 7) REORDER & WRITE MERGED ─────────────────────────────────────────────────
cols        = final_df.columns.tolist()
front       = ["deals.Deal ID","deals.Company ID","deals.Investor ID"]
deals_cols  = [c for c in cols if c.startswith("deals.") and c not in front]
inv_cols    = [c for c in cols if c.startswith("inv.")]
comp_cols   = [c for c in cols if c.startswith("comp.")]
people_cols = [c for c in cols if c.startswith("people.")]
file_cols   = [c for c in cols if c.endswith(".file_source") or c.endswith(".file_source_type")]

ordered = front + deals_cols + inv_cols + comp_cols + people_cols + file_cols
final_df[ordered].to_excel(OUTPUT_MERGED, index=False)
print("✅ Merged written to", OUTPUT_MERGED)


# ─── 8) DROP BLANK/UNNAMED & WRITE REFINED ───────────────────────────────────────
def is_blank_or_na(s):
    return s.isna().all() or s.astype(str).str.strip().eq("").all()

to_drop = [c for c in final_df.columns if c.startswith("Unnamed") or is_blank_or_na(final_df[c])]
refined = final_df.drop(columns=sorted(to_drop))
refined.to_excel(OUTPUT_REFINED, index=False)
print("✅ Refined written to", OUTPUT_REFINED)


# ─── 9) VALIDATION SUMMARY ───────────────────────────────────────────────────────
print("\n--- Validation Summary ---")
print("> Deals sources:    ", sorted(final_df["deals.file_source"].dropna().unique()))
print("> Company sources:  ", sorted(final_df["comp.file_source"].dropna().unique()))
print("> Investor sources: ", sorted(final_df["inv.file_source"].dropna().unique()))
print("> People sources:   ", sorted(final_df["people.file_source"].dropna().unique()))
print("> Mapping sources:  ", sorted(mapping_df["map.file_source"].dropna().unique()))

inv_union = pd.concat(
    [pd.read_excel(f, header=detect_header_row(f, ["Investor ID"])) for f in investor_files],
    ignore_index=True
)
print("\nTotal investor rows (union):", inv_union.shape[0])
print("Unique Investor IDs:       ", inv_union["Investor ID"].dropna().nunique())
print("Merged rows:               ", final_df.shape[0])
