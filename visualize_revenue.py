import pandas as pd
import matplotlib.pyplot as plt

# Load the borough revenue CSV
df = pd.read_csv("borough_revenue_2019.csv")

# Aggregate total revenue per borough
revenue_by_borough = df.groupby("PUBorough")["revenue"].sum().sort_values(ascending=False)

# Plot
plt.figure(figsize=(8,5))
revenue_by_borough.plot(kind="bar", color="skyblue", edgecolor="black")
plt.title("NYC Yellow Taxi Revenue by Borough (2019 Jan–Mar)")
plt.xlabel("Pickup Borough")
plt.ylabel("Total Revenue (USD)")
plt.xticks(rotation=45)
plt.tight_layout()

# Save figure
plt.savefig("borough_revenue_chart.png")
plt.show()
