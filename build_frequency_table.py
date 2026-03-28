import pandas as pd

print("Loading clean files...")
train   = pd.read_csv("train_reviews.csv")
files   = pd.read_csv("files_clean.csv")

print(f"Train reviews: {len(train)} rows")
print(f"Files: {len(files)} rows")

print("\nJoining reviews with files...")
train_files = train[["ch_id", "ch_authorAccountId"]].merge(files, on="ch_id", how="inner")
print(f"Joined table: {len(train_files)} rows")

print("\nBuilding frequency table...")
freq = train_files.groupby(["ch_authorAccountId", "filename"]).size().reset_index(name="count")
freq.columns = ["author", "file_path", "count"]

freq.to_csv("frequency_table.csv", index=False)

print(f"\nDone! Frequency table saved: {len(freq)} rows")
print("\nTop 10 most active developer-file pairs:")
print(freq.sort_values("count", ascending=False).head(10).to_string())