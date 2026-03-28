import pandas as pd

print("Loading files...")
history_raw = pd.read_csv("history_raw.csv", header=None,
                           low_memory=False, on_bad_lines='skip')
history_raw = history_raw.iloc[1:]

history = history_raw[[0, 3]].copy()
history.columns = ["ch_id", "reviewer_id"]
history = history.dropna()
history["ch_id"] = history["ch_id"].astype(str).str.replace('.0', '', regex=False)
history["reviewer_id"] = history["reviewer_id"].astype(str).str.replace('.0', '', regex=False)
print(f"Raw history: {len(history)} rows")

print("Loading people...")
people = pd.read_csv("people_raw.csv", header=None)
people = people[[1, 2]]
people.columns = ["account_id", "full_name"]
people["account_id"] = people["account_id"].astype(str).str.replace('.0', '', regex=False)

print("Mapping IDs to real names...")
history = history.merge(people, left_on="reviewer_id", right_on="account_id", how="left")
history = history[["ch_id", "full_name"]]
history.columns = ["ch_id", "reviewer"]

# remove bots - case insensitive
bot_keywords = ["jenkins", "zuul", "smokestack", "bot", "ci", 
                "infra", "proposal", "openstack.org", "review"]

history = history[~history["reviewer"].apply(
    lambda x: any(keyword in str(x).lower() for keyword in bot_keywords)
)]

test = pd.read_csv("test_reviews.csv")
test["ch_id"] = test["ch_id"].astype(str)
history["ch_id"] = history["ch_id"].astype(str)
matches = history["ch_id"].isin(test["ch_id"]).sum()
print(f"Rows matching test reviews: {matches}")

history.to_csv("history_clean.csv", index=False)
print("\nSaved history_clean.csv")
print("\nSample:")
print(history.head(10).to_string())