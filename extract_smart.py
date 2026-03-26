import re
import pandas as pd

def extract_table_chunked(filename, table_name, output_csv, columns=None):
    print(f"Extracting {table_name}...")
    pattern = re.compile(
        rf"INSERT INTO `{table_name}` VALUES\s*\((.+?)\);", re.DOTALL
    )
    rows = []
    chunk_size = 1024 * 1024  # 1MB at a time
    buffer = ""
    count = 0

    with open(filename, "r", encoding="utf-8", errors="ignore") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            buffer += chunk
            # Process complete INSERT statements only
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
                    rows.append(values)
                    count += 1
                # Remove processed part from buffer
                buffer = buffer[match.end():]

            # Save in batches to avoid memory issues
            if len(rows) > 50000:
                df = pd.DataFrame(rows, columns=columns)
                df.to_csv(output_csv, mode='a',
                         header=not pd.io.common.file_exists(output_csv) or count <= len(rows),
                         index=False)
                rows = []
                print(f"  ...saved {count} rows so far")

    # Save remaining rows
    if rows:
        df = pd.DataFrame(rows, columns=columns)
        df.to_csv(output_csv, mode='a', index=False, header=count <= len(rows))

    print(f"✅ Saved {output_csv} — {count} total rows\n")

import os

# Remove old files first
for f in ["history_raw.csv", "people_raw.csv", "revision_raw.csv"]:
    if os.path.exists(f):
        os.remove(f)

extract_table_chunked("openstack20161121.sql", "t_history", "history_raw.csv")
extract_table_chunked("openstack20161121.sql", "t_people",  "people_raw.csv")
extract_table_chunked("openstack20161121.sql", "t_revision","revision_raw.csv")

print("🎉 ALL DONE!")