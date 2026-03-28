import re
import pandas as pd

print("Extracting reviews from SQL - stopping early...")

rows = []
pattern = re.compile(
    r"INSERT INTO `t_change` VALUES\s*\((.+?)\);", re.DOTALL
)

chunk_size = 1024 * 1024
buffer = ""
total_mb = 0
done = False

with open("openstack20161121-1.sql", "r", encoding="utf-8", errors="ignore") as f:
    while not done:
        chunk = f.read(chunk_size)
        if not chunk:
            break
        buffer += chunk
        total_mb += 1

        if total_mb % 50 == 0:
            print(f"  Read {total_mb}MB... found {len(rows)} reviews")

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

            # stop as soon as we have all reviews
            if len(rows) >= 228099:
                print(f"\nGot all reviews at {total_mb}MB — stopping!")
                done = True
                break

print(f"\nExtracted {len(rows)} reviews")

# convert to dataframe
df = pd.DataFrame(rows)
print("Sample:")
print(df.head(3).to_string())
print("\nColumns:")
for i in range(len(df.columns)):
    print(f"  Col {i}: {df[i].head(3).tolist()}")

df.to_csv("reviews_raw.csv", index=False)
print("\nSaved reviews_raw.csv")