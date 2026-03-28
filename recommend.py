import pandas as pd

print("Loading files...")
freq        = pd.read_csv("frequency_table.csv")
test        = pd.read_csv("test_reviews.csv")
history     = pd.read_csv("history_clean.csv", low_memory=False)
files_clean = pd.read_csv("files_clean.csv")

# fix types — make everything strings for consistent matching
history["ch_id"]    = history["ch_id"].astype(str).str.replace('.0','', regex=False).str.strip()
test["ch_id"]       = test["ch_id"].astype(str).str.replace('.0','', regex=False).str.strip()
files_clean["ch_id"] = files_clean["ch_id"].astype(str).str.replace('.0','', regex=False).str.strip()

# fix the number '2' appearing — remove any reviewer that is just a number
history = history[~history["reviewer"].astype(str).str.match(r'^\d+$')]

# group files by review ID for speed
print("Grouping files by review...")
files_by_review = files_clean.groupby("ch_id")["filename"].apply(list).to_dict()

# group actual reviewers by review ID
print("Grouping actual reviewers by review...")
actual_by_review = history.groupby("ch_id")["reviewer"].apply(list).to_dict()

# check matches
matches = sum(1 for ch_id in test["ch_id"] if ch_id in actual_by_review)
print(f"Test reviews with actual reviewers: {matches}")

def get_directory(file_path):
    if not isinstance(file_path, str) or "/" not in file_path:
        return ""
    return "/".join(file_path.split("/")[:-1])

def recommend(changed_files, author, top_n=3):
    matches = freq[freq["file_path"].isin(changed_files)]

    if matches.empty:
        directories = [get_directory(f) for f in changed_files]
        matches = freq[freq["file_path"].apply(
            lambda x: any(str(x).startswith(d) for d in directories if d)
        )]

    if matches.empty:
        return []

    scores = matches.groupby("author")["count"].sum()
    scores = scores[scores.index != author]
    # remove any numeric entries
    scores = scores[~scores.index.astype(str).str.match(r'^\d+$')]

    return scores.nlargest(top_n).index.tolist()

print(f"Running recommender on {len(test)} test reviews...")
results = []

for i, row in test.iterrows():
    rev_id = str(row["ch_id"]).replace('.0','').strip()
    author = row["ch_authorAccountId"]

    changed_files    = files_by_review.get(rev_id, [])
    actual_reviewers = actual_by_review.get(rev_id, [])
    recommended      = recommend(changed_files, author)

    results.append({
        "ch_id":         rev_id,
        "author":        author,
        "files_changed": ", ".join(changed_files),
        "recommended":   ", ".join(recommended),
        "actual":        ", ".join(actual_reviewers)
    })

    if (i + 1) % 10000 == 0:
        print(f"  Processed {i + 1} reviews...")

output = pd.DataFrame(results)
output.to_csv("recommendations_output.csv", index=False)

print(f"\nDone! Saved recommendations_output.csv")
print(f"\nSample output:")
print(output.head(10).to_string())

# quick stats
has_actual = output["actual"].str.len() > 0
has_recommended = output["recommended"].str.len() > 0
print(f"\nReviews with actual reviewers: {has_actual.sum()}")
print(f"Reviews with recommendations: {has_recommended.sum()}")