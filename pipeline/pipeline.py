import sys
import pandas as pd

print("arguments", sys.argv)

day = int(sys.argv[1])
print(f"Running pipeline for day {day}")

# create the dataframe with 2 columns and 2 rows (like 1, 2)
#                   V            V
df = pd.DataFrame({"A": [1, 2], "B": [3, 4]})
print(df.head())

# PARQUET format
# - smaller size
# - system reads only columns you need
# - schema preservation (not just all text treated like CSV)
# 
df.to_parquet(f"output_day_{sys.argv[1]}.parquet")