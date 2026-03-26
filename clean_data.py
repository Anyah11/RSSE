import re
import pandas as pd

# ---- STEP 1: Read the SQL file ----
print("Reading SQL file... this may take a minute")
with open("openstack20161121.sql", "r", encoding="utf-8", errors="ignore") as f:
    content = f.read()
print("Done reading file!")

# ---- STEP 2: Extract t_change table (code reviews) ----
print("Extracting code reviews...")
change_pattern = re.compile(
    r"INSERT INTO `t_change` VALUES\s*\((.+?)\);", re.DOTALL
)
change_rows = []
for match in change_pattern.finditer(content):
    rows_text = match.group(1)
    # Split multiple rows in one INSERT
    rows = re.split(r"\),\s*\(", rows_text)
    for row in rows:
        row = row.strip().strip("()")
        # Parse values carefully
        values = re.findall(r"'(?:[^'\\]|\\.)*'|NULL|\d+", row)
        values = [v.strip("'") for v in values]
        change_rows.append(values)

change_df = pd.DataFrame(change_rows, columns=[
    "id", "ch_id", "ch_changeId", "ch_changeIdNum", "ch_project",
    "ch_branch", "ch_topic", "ch_authorAccountId", "ch_createdTime",
    "ch_updatedTime", "ch_status", "ch_mergeable"
])
change_df.to_csv("reviews_raw.csv", index=False)
print(f"Saved reviews_raw.csv — {len(change_df)} rows")

# ---- STEP 3: Extract t_file table (which files changed) ----
print("Extracting file changes...")
file_pattern = re.compile(
    r"INSERT INTO `t_file` VALUES\s*\((.+?)\);", re.DOTALL
)
file_rows = []
for match in file_pattern.finditer(content):
    rows_text = match.group(1)
    rows = re.split(r"\),\s*\(", rows_text)
    for row in rows:
        row = row.strip().strip("()")
        values = re.findall(r"'(?:[^'\\]|\\.)*'|NULL|\d+", row)
        values = [v.strip("'") for v in values]
        file_rows.append(values)

file_df = pd.DataFrame(file_rows)
file_df.to_csv("files_raw.csv", index=False)
print(f"Saved files_raw.csv — {len(file_df)} rows")

# ---- STEP 4: Extract t_patch table (who made changes) ----
print("Extracting patch/reviewer info...")
patch_pattern = re.compile(
    r"INSERT INTO `t_patch` VALUES\s*\((.+?)\);", re.DOTALL
)
patch_rows = []
for match in patch_pattern.finditer(content):
    rows_text = match.group(1)
    rows = re.split(r"\),\s*\(", rows_text)
    for row in rows:
        row = row.strip().strip("()")
        values = re.findall(r"'(?:[^'\\]|\\.)*'|NULL|\d+", row)
        values = [v.strip("'") for v in values]
        patch_rows.append(values)

patch_df = pd.DataFrame(patch_rows)
patch_df.to_csv("patches_raw.csv", index=False)
print(f"Saved patches_raw.csv — {len(patch_df)} rows")

print("\n✅ ALL DONE! Check your folder for the 3 CSV files.")