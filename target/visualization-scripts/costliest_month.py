import pandas
import matplotlib as mpl
mpl.use("TKAgg")
import matplotlib.pyplot as plt
import os
import sys

file_found = False

# read the daily spending of last 60 days csv file
for filename in os.listdir(os.getcwd()):
   if filename.endswith('.csv'):
      df = pandas.read_csv(filename)
      file_found = True

# if the csv is not there quit the application
if not file_found:
   print("No csv file found")
   sys.exit()

# convert cost from string to numeric
df["cost"] = pandas.to_numeric(df["cost"].str.replace(',',''), errors='coerce')


df['monthyear'] = df["month"] + " " + df["year"].astype(str)


# plot costliest month
ax = df.plot(kind='bar', x='monthyear', y='cost', title ="Costliest Month", figsize=(8, 9), legend=True, fontsize=11)
ax.set_xlabel("Month", fontsize=12)
ax.set_ylabel("Cost (Euros)", fontsize=12)
plt.xticks(rotation=0)
plt.show()

