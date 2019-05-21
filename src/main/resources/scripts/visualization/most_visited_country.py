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

# convert count from string to numeric
df["Count"] = pandas.to_numeric(df["Count"])


# plot most visited country
ax = df.plot(kind='bar', x='location', y='Count', title ="Most Visited Country", figsize=(8, 9), legend=True, fontsize=11)
ax.set_xlabel("Country", fontsize=12)
ax.set_ylabel("Times Visited", fontsize=12)
plt.show()

