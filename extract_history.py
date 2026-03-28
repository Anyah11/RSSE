import re
import pandas as pd

print("Extracting history - streaming mode...")

rows = []
chunk_size = 1024 * 1024
buffer = ""
found = 0

pattern = re.compile(
    r"INSERT INTO `t_history` VALUES\s*\((.+?)\);", re.DOTALL
)

with open("openstack20161121-1.sql", "r", encoding="utf-8", errors="ignore") as f:
    while True:
        chunk = f.read(chunk_size)
        if not chunk:
            break
        buffer += chunk

        while True:
            match = pattern.search(buffer)
            if not match:
                break
            rows_text = match.group(1)
            split_rows = re.split(r"\),\s*\(", rows_text)
            for row in split_rows:
                row = row.strip().strip("()")
                values = re.findall(r"'(?:[^'\\]|\\.)*'|NULL|-?\d+", row)
                values = [v.strip("'") for v in values]
                # only keep what we need - all columns for now
                rows.append(values)
                found += 1
            buffer = buffer[match.end():]

        if found % 500000 == 0 and found > 0:
            print(f"  Found {found} rows so far...")

        # save in batches to avoid memory issues
        if len(rows) >= 200000:
            batch = pd.DataFrame(rows)
            batch.to_csv("history_raw.csv", 
                        mode='a',
                        header=not pd.io.common.file_exists("history_raw.csv"),
                        index=False)
            rows = []
            print(f"  Saved batch, total {found} rows...")

# save remaining rows
if rows:
    batch = pd.DataFrame(rows)
    batch.to_csv("history_raw.csv",
                mode='a',
                header=not pd.io.common.file_exists("history_raw.csv"),
                index=False)

print(f"\nDone! Total {found} rows extracted")
print("\nChecking columns...")
sample = pd.read_csv("history_raw.csv", nrows=5)
print(sample.to_string())