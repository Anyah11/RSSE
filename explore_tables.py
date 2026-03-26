import re

print("Scanning SQL file for all table names...")
with open("openstack20161121.sql", "r", encoding="utf-8", errors="ignore") as f:
    content = f.read()

# Find all table names that have INSERT INTO statements
tables = re.findall(r"INSERT INTO `(\w+)` VALUES", content)
unique_tables = list(set(tables))
print("Tables found with data:")
for t in sorted(unique_tables):
    count = content.count(f"INSERT INTO `{t}`")
    print(f"  {t} — {count} INSERT statements")