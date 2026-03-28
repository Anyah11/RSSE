import pandas as pd

print("Loading files...")
reviews = pd.read_csv("reviews_raw.csv", low_memory=False, header=None, skiprows=1)
reviews = reviews[[0, 7, 8, 10]]
reviews.columns = ["ch_id", "ch_authorAccountId", "ch_createdTime", "ch_status"]
reviews["ch_createdTime"] = pd.to_datetime(reviews["ch_createdTime"], errors="coerce")
reviews = reviews.dropna(subset=["ch_createdTime"])
reviews["ch_id"] = reviews["ch_id"].astype(str).str.replace('.0', '', regex=False)
reviews["ch_authorAccountId"] = reviews["ch_authorAccountId"].astype(str).str.replace('.0', '', regex=False)

print(f"Total reviews: {len(reviews)}")

# split train/test
train = reviews[reviews["ch_createdTime"].dt.year < 2015]
test  = reviews[reviews["ch_createdTime"].dt.year == 2015]
print(f"Train: {len(train)} | Test: {len(test)}")

# load people
people = pd.read_csv("people_raw.csv", header=None, skiprows=2)
people = people[[1, 2]]
people.columns = ["account_id", "full_name"]
people["account_id"] = people["account_id"].astype(str).str.replace('.0', '', regex=False)

# remove bots
bot_keywords = ["jenkins", "zuul", "smokestack", "bot", "ci",
                "infra", "proposal", "openstack.org", "review"]
people = people[~people["full_name"].apply(
    lambda x: any(k in str(x).lower() for k in bot_keywords)
)]
print(f"People: {len(people)} rows")

# map names onto test
test = test.merge(people, left_on="ch_authorAccountId", right_on="account_id", how="left")
test["ch_authorAccountId"] = test["full_name"]
test = test.drop(columns=["account_id", "full_name"])

# map names onto train
train = train.merge(people, left_on="ch_authorAccountId", right_on="account_id", how="left")
train["ch_authorAccountId"] = train["full_name"]
train = train.drop(columns=["account_id", "full_name"])

print(f"\nTest author sample:")
print(test["ch_authorAccountId"].head(10).tolist())
named = test["ch_authorAccountId"].notna().sum()
print(f"Named: {named} out of {len(test)}")

# save
test.to_csv("test_reviews.csv", index=False)
train.to_csv("train_reviews.csv", index=False)
print("\nSaved test_reviews.csv and train_reviews.csv")