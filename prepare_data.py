import pandas as pd

print("Loading data... this may take a minute")

# ---- Load reviews ----
reviews = pd.read_csv("reviews_raw.csv", header=None, low_memory=False, on_bad_lines='skip')
reviews = reviews.iloc[1:]  # skip header row
reviews.columns = list(reviews.columns)
reviews = reviews[[0, 7, 8, 10]]
reviews.columns = ["ch_id", "ch_authorAccountId", "ch_createdTime", "ch_status"]
reviews["ch_createdTime"] = pd.to_datetime(reviews["ch_createdTime"], errors="coerce")
reviews = reviews.dropna(subset=["ch_createdTime"])
print(f"Reviews loaded: {len(reviews)} rows")

# ---- Load files ----
files = pd.read_csv("files_raw.csv", header=None, low_memory=False, on_bad_lines='skip')
print(f"Files columns: {len(files.columns)}")
print("Files first row:", files.iloc[0].tolist())
print("Files second row:", files.iloc[1].tolist())
files = files.iloc[1:]  # skip header row
files = files[[0, 1]]
files.columns = ["ch_id", "filename"]
files = files.dropna(subset=["filename"])
files = files[files["filename"].astype(str).str.len() > 3]
print(f"Files loaded: {len(files)} rows")

# ---- Load history (reviewer activity) ----
history = pd.read_csv("history_raw.csv", header=None, low_memory=False, on_bad_lines='skip')
history = history.iloc[1:]  # skip header row
history = history[[1, 3]]
history.columns = ["ch_id", "reviewer_id"]
history = history.dropna(subset=["reviewer_id"])
history = history[history["reviewer_id"].astype(str).str.match(r'^\d+\.?0*$')]
history["reviewer_id"] = history["reviewer_id"].astype(str).str.replace('.0','', regex=False)
print(f"History loaded: {len(history)} rows")

# ---- Train/Test Split ----
print("\nSplitting into train/test...")
train_reviews = reviews[reviews["ch_createdTime"].dt.year < 2015]
test_reviews  = reviews[reviews["ch_createdTime"].dt.year == 2015]
print(f"Training reviews (2011-2014): {len(train_reviews)}")
print(f"Testing reviews  (2015):      {len(test_reviews)}")

# ---- Save clean files ----
train_reviews.to_csv("train_reviews.csv", index=False)
test_reviews.to_csv("test_reviews.csv", index=False)
files.to_csv("files_clean.csv", index=False)
history.to_csv("history_clean.csv", index=False)

print("\n✅ DONE! Clean files saved:")
print("  train_reviews.csv")
print("  test_reviews.csv")
print("  files_clean.csv")
print("  history_clean.csv")

print("\n--- Sample from train_reviews ---")
print(train_reviews.head(3).to_string())
print("\n--- Sample from files_clean ---")
print(files.head(3).to_string())
print("\n--- Sample from history_clean ---")
print(history.head(3).to_string())