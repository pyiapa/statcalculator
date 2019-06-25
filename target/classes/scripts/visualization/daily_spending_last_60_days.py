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

# plot daily cost for the last 60 days
ax = df.plot(kind='bar', x='date', y='cost', title ="Cost last 60 days", figsize=(15, 10), legend=True, fontsize=11)
ax.set_xlabel("Date", fontsize=12)
ax.set_ylabel("Cost (Euros)", fontsize=12)
ax.set_xticklabels(df["date"], rotation = 55, ha="right")
plt.grid()
plt.show()

