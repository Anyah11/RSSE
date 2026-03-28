import re
import pandas as pd

print("Extracting people table...")

rows = []
pattern = re.compile(
    r"INSERT INTO `t_people` VALUES\s*\((.+?)\);", re.DOTALL
)

chunk_size = 1024 * 1024
buffer = ""
total_mb = 0

with open("openstack20161121-1.sql", "r", encoding="utf-8", errors="ignore") as f:
    while True:
        chunk = f.read(chunk_size)
        if not chunk:
            break
        buffer += chunk
        total_mb += 1

        if total_mb % 10 == 0:
            print(f"  Read {total_mb}MB... found {len(rows)} people so far")

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
            buffer = buffer[match.end():]

        if len(rows) >= 8089:
            print(f"\nFound all {len(rows)} people at {total_mb}MB — stopping early!")
            break

df = pd.DataFrame(rows)
df.to_csv("people_raw.csv", index=False)
print(f"\nSaved people_raw.csv — {len(df)} rows")
print("Sample:")
print(df.head(5).to_string())