import re
import pandas as pd

print("Reading SQL file...")
with open("openstack20161121.sql", "r", encoding="utf-8", errors="ignore") as f:
    content = f.read()
print("Done!\n")

def extract_table(content, table_name):
    pattern = re.compile(
        rf"INSERT INTO `{table_name}` VALUES\s*\((.+?)\);", re.DOTALL
    )
    rows = []
    for match in pattern.finditer(content):
        rows_text = match.group(1)
        split_rows = re.split(r"\),\s*\(", rows_text)
        for row in split_rows:
            row = row.strip().strip("()")
            values = re.findall(r"'(?:[^'\\]|\\.)*'|NULL|-?\d+", row)
            values = [v.strip("'") for v in values]
            rows.append(values)
    return rows

# ---- Extract t_history (reviewer activity) ----
print("Extracting t_history (reviewer activity)...")
history_rows = extract_table(content, "t_history")
history_df = pd.DataFrame(history_rows)
history_df.to_csv("history_raw.csv", index=False)
print(f"Saved history_raw.csv — {len(history_df)} rows")
print("First row sample:", history_rows[0] if history_rows else "empty")
print()

# ---- Extract t_people (developer info) ----
print("Extracting t_people (developer accounts)...")
people_rows = extract_table(content, "t_people")
people_df = pd.DataFrame(people_rows)
people_df.to_csv("people_raw.csv", index=False)
print(f"Saved people_raw.csv — {len(people_df)} rows")
print("First row sample:", people_rows[0] if people_rows else "empty")
print()

# ---- Extract t_revision (review versions) ----
print("Extracting t_revision...")
revision_rows = extract_table(content, "t_revision")
revision_df = pd.DataFrame(revision_rows)
revision_df.to_csv("revision_raw.csv", index=False)
print(f"Saved revision_raw.csv — {len(revision_df)} rows")
print("First row sample:", revision_rows[0] if revision_rows else "empty")
print()

print("✅ ALL DONE!")