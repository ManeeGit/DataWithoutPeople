#!/usr/bin/env python3
import pandas as pd
import os
import re
from rapidfuzz import process, fuzz

# ─── 1) INPUT FILE LISTS ─────────────────────────────────────────────────────────
deal_files = [
    "deals_PitchBook_Search_Result_Columns_2024_11_21_16_55_11.xlsx",
    "deals_PitchBook_Search_Result_Columns_2024_12_12_14_00_02.xlsx",
]
company_files = [
    "companies_PitchBook_Search_Result_Columns_2024_11_21_16_55_55.xlsx",
    "companies_PitchBook_Search_Result_Columns_2024_12_12_13_56_26.xlsx",
]
investor_files = [
    "investors_PitchBook_Search_Result_Columns_2024_11_21_17_00_53.xlsx",
    "investors_PitchBook_Search_Result_Columns_2024_12_30_13_49_02.xlsx",
    "investors2_PitchBook_Search_Result_Columns_2024_11_25_19_40_00.xlsx",
    "investors3_PitchBook_All_Columns_2025_05_30_15_45_26.xlsx",
]
people_files = [
    "people_PitchBook_Search_Result_Columns_2024_11_21_17_01_48.xlsx",
    "people_PitchBook_Search_Result_Columns_2024_12_12_14_08_01.xlsx",
]
mapping_files = [
    "PitchBook_Deal_Investors_2025_05_29_23_02_47.xlsx",
    "PitchBook_Deal_Investors_2025_05_30_20_36_20.xlsx",
]

OUTPUT_MERGED  = "final_merged_deals_companies_investors_people.xlsx"
OUTPUT_REFINED = "final_refined_deals_companies_investors_people.xlsx"
DEBUG_OVERLAP  = "join_overlap.csv"

# ─── 2) HEADER DETECTION ──────────────────────────────────────────────────────────
def detect_header_row(path, id_cols):
    preview = pd.read_excel(path, header=None, nrows=20)
    for i, row in preview.iterrows():
        tokens = {str(x).strip() for x in row.dropna().astype(str)}
        if any(c in tokens for c in id_cols):
            return i
    raise ValueError(f"No header matching {id_cols} in first 20 rows of {path}")

# ─── 3) GENERIC LOADER ────────────────────────────────────────────────────────────
def load_and_prefix(files, prefix, ftype, id_cols, usecols=None):
    dfs = []
    for fn in files:
        if not os.path.exists(fn):
            raise FileNotFoundError(f"Missing file: {fn}")
        hdr = detect_header_row(fn, id_cols)
        df  = pd.read_excel(fn, header=hdr, usecols=usecols, dtype=str)
        df.columns = df.columns.map(str).str.strip()
        df[f"{prefix}file_source"]      = os.path.basename(fn)
        df[f"{prefix}file_source_type"] = ftype
        df = df.rename(columns=lambda c: f"{prefix}{c}" if not c.startswith(prefix) else c)
        dfs.append(df)
    return pd.concat(dfs, ignore_index=True).drop_duplicates(ignore_index=True)

# ─── 4) LOAD INVESTORS & PEOPLE FOR OVERLAP ANALYSIS ─────────────────────────────
print("🔍  Loading investors & people for overlap analysis…")
inv_raw    = load_and_prefix(investor_files, "inv.",    "investors", ["Investor ID","Investor Legal Name","PBId"])
people_raw = load_and_prefix(people_files,   "people.", "people",   ["PBId","Primary Company"])

# normalize keys
inv_raw["inv.Investor ID"]         = inv_raw["inv.Investor ID"].str.strip().fillna("")
inv_raw["inv.Investor Legal Name"] = inv_raw["inv.Investor Legal Name"].str.strip().fillna("")
inv_raw["inv.PBId"]                = inv_raw["inv.PBId"].str.strip().fillna("")
people_raw["people.PBId"]          = people_raw["people.PBId"].str.strip().fillna("")
people_raw["people.Primary Company"]= people_raw["people.Primary Company"].str.strip().fillna("")

# candidate join keys
inv_keys   = ["inv.Investor ID","inv.Investor Legal Name","inv.PBId"]
people_keys= ["people.PBId","people.Primary Company"]

# compute exact overlaps
rows=[]
inv_sets   = {k:set(inv_raw[k].unique()) for k in inv_keys}
people_sets= {k:set(people_raw[k].unique()) for k in people_keys}
for ik in inv_keys:
    for pk in people_keys:
        common = inv_sets[ik].intersection(people_sets[pk])
        rows.append({
            "investor_col": ik,
            "people_col":   pk,
            "inv_unique":   len(inv_sets[ik]),
            "ppl_unique":   len(people_sets[pk]),
            "common":       len(common),
            "inv_pct":      len(common)/len(inv_sets[ik]) if inv_sets[ik] else 0,
            "ppl_pct":      len(common)/len(people_sets[pk]) if people_sets[pk] else 0,
        })
overlap = pd.DataFrame(rows).sort_values("common", ascending=False)
overlap.to_csv(DEBUG_OVERLAP, index=False)

print("\n=== EXACT JOIN OVERLAP ===")
print(overlap.head(10).to_string(
    index=False,
    formatters={"inv_pct":"{:.1%}".format,"ppl_pct":"{:.1%}".format}
))
print(f"👉  Details in {DEBUG_OVERLAP}")

# ─── 5) BUILD FUZZY MAP ───────────────────────────────────────────────────────────
print("\n🔍  Building fuzzy‐match map (Investor Legal Name → People Primary Company)…")
def normalize_text(s):
    return re.sub(r"[^a-z0-9 ]","",str(s).lower()).strip()

inv_names = inv_raw["inv.Investor Legal Name"].dropna().unique()
people_names = people_raw["people.Primary Company"].dropna().unique()

inv_norm = [normalize_text(n) for n in inv_names]
pe_norm  = [normalize_text(n) for n in people_names]

threshold = 85
fuzzy_map = {}
for invn, invn_norm in zip(inv_names, inv_norm):
    best, score, _ = process.extractOne(
        invn_norm, pe_norm, scorer=fuzz.token_sort_ratio
    )
    if score >= threshold:
        orig = people_names[pe_norm.index(best)]
        fuzzy_map[invn] = orig
    else:
        fuzzy_map[invn] = None

inv_raw["fuzzy_people"] = inv_raw["inv.Investor Legal Name"].map(fuzzy_map)
matched_count = inv_raw["fuzzy_people"].notna().sum()
print(f"✅  {matched_count} of {len(inv_raw)} investor rows matched fuzzily (≥{threshold}%).")

# ─── 6) LOAD & UNION ALL CATEGORIES ───────────────────────────────────────────────
print("\n🔄  Loading & prefixing all categories…")
deals_df     = load_and_prefix(deal_files,     "deals.",  "deals",     ["Deal ID"])
companies_df = load_and_prefix(company_files,  "comp.",   "companies", ["Company ID"])
investors_df = inv_raw.copy()
people_df    = load_and_prefix(people_files,   "people.", "people",   ["PBId","Primary Company"])

map_dfs=[]
for fn in mapping_files:
    hdr = detect_header_row(fn, ["Deal ID","Company ID","Investor ID"])
    m   = pd.read_excel(fn, header=hdr, usecols=["Deal ID","Company ID","Investor ID"], dtype=str)
    m2  = m.drop_duplicates()
    m2["map.file_source"]      = os.path.basename(fn)
    m2["map.file_source_type"] = "mapping"
    map_dfs.append(m2)
mapping_df = (
    pd.concat(map_dfs,ignore_index=True)
      .drop_duplicates()
      .rename(columns={
          "Deal ID":"map.Deal ID",
          "Company ID":"map.Company ID",
          "Investor ID":"map.Investor ID"
      })
)
mapping_df["map.Investor ID"] = mapping_df["map.Investor ID"].str.strip()

for df,c in [
    (investors_df,"inv.Investor ID"),
    (investors_df,"inv.Investor Legal Name"),
    (deals_df,"deals.Deal ID"),
    (companies_df,"comp.Company ID"),
    (mapping_df,"map.Deal ID"),
    (mapping_df,"map.Company ID"),
    (mapping_df,"map.Investor ID"),
]:
    if c in df.columns:
        df[c] = df[c].astype(str).str.strip()

people_df["people.Primary Company_norm"] = (
    people_df["people.Primary Company"]
    .astype(str).str.lower()
    .str.replace(r"[^a-z0-9 ]","",regex=True)
    .str.strip()
)

# ─── 7) SEQUENTIAL MERGE ─────────────────────────────────────────────────────────
print("\n🔗  Merging mapping → deals → investors → companies…")
m1 = mapping_df.merge(deals_df,
                      left_on="map.Deal ID", right_on="deals.Deal ID", how="left")
m2 = m1.merge(investors_df,
              left_on="map.Investor ID", right_on="inv.Investor ID", how="left")
m3 = m2.merge(companies_df,
              left_on="map.Company ID", right_on="comp.Company ID", how="left")

print("🔗  …now merging in people via fuzzy map…")
merged = m3.merge(
    people_df,
    left_on="fuzzy_people",
    right_on="people.Primary Company",
    how="left",
    suffixes=("","_p2")
)

# ─── 8) CLEAN & DEDUPE ────────────────────────────────────────────────────────────
print("🧹  Cleaning up and deduplicating…")
merged["deals.Investor ID"] = merged["map.Investor ID"]
final_df = (
    merged
      .drop(columns=["map.Deal ID","map.Company ID","map.Investor ID","fuzzy_people"])
      .drop_duplicates(
          subset=["deals.Deal ID","deals.Investor ID","deals.Company ID"],
          ignore_index=True
      )
)

# ─── 9) REORDER & SAVE FULL MERGED ───────────────────────────────────────────────
print("💾  Writing full merged →", OUTPUT_MERGED)
all_cols    = final_df.columns.tolist()
front       = ["deals.Deal ID","deals.Company ID","deals.Investor ID"]
deals_cols  = [c for c in all_cols if c.startswith("deals.") and c not in front]
inv_cols    = [c for c in all_cols if c.startswith("inv.")]
comp_cols   = [c for c in all_cols if c.startswith("comp.")]
people_cols = [c for c in all_cols if c.startswith("people.")]
file_cols   = [c for c in all_cols if c.endswith(".file_source") or c.endswith(".file_source_type")]

ordered = front + deals_cols + inv_cols + comp_cols + people_cols + file_cols
final_df[ordered].to_excel(OUTPUT_MERGED, index=False)

# ─── 10) REFINE & SAVE ───────────────────────────────────────────────────────────
def is_blank_or_na(s):
    return s.isna().all() or s.astype(str).str.strip().eq("").all()

to_drop = [c for c in final_df.columns if c.startswith("Unnamed") or is_blank_or_na(final_df[c])]
refined = final_df.drop(columns=sorted(to_drop))
print("💾  Writing refined →", OUTPUT_REFINED)
refined.to_excel(OUTPUT_REFINED, index=False)

# ─── 11) VALIDATION SUMMARY ──────────────────────────────────────────────────────
print("\n--- Validation Summary ---")
print("Deals sources:     ", sorted(final_df["deals.file_source"].dropna().unique()))
print("Company sources:   ", sorted(final_df["comp.file_source"].dropna().unique()))
print("Investor sources:  ", sorted(final_df["inv.file_source"].dropna().unique()))
print("People sources:    ", sorted(people_df["people.file_source"].dropna().unique()))
print("Mapping sources:   ", sorted(mapping_df["map.file_source"].dropna().unique()))

inv_union = pd.concat(
    [pd.read_excel(f, header=detect_header_row(f, ["Investor ID"])) for f in investor_files],
    ignore_index=True
)
print("\nTotal investor rows (union):", inv_union.shape[0])
print("Unique Investor IDs:          ", inv_union["Investor ID"].dropna().nunique())
print("Merged rows:                  ", final_df.shape[0])

print("\n✅ All done! 🎉")
