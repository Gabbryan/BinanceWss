import logging
import os

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from matplotlib.ticker import FuncFormatter
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from sklearn.preprocessing import StandardScaler

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)
# Set display options for maximum rows and columns
pd.set_option(
    "display.max_rows", None
)  # Set to None to display all rows, or specify a number
pd.set_option("display.max_columns", None)  # Set to None to display all columns
pd.set_option("display.max_colwidth", None)  # Display the full width of each column
pd.set_option("display.width", None)  # Allow for automatic width adjustment
logger.info("Loading data from Parquet file")
# Load the Parquet file
# Initialize an empty list to hold DataFrames
dfs = []

# Define the root directory where the Parquet files are stored
root_dir = "./OUTPUT"

# Walk through the directory structure
for root, dirs, files in os.walk(root_dir):
    for file in files:
        if file.endswith(".parquet"):
            file_path = os.path.join(root, file)
            try:
                logger.info(f"Loading data from {file_path}")
                # Load each Parquet file into a DataFrame and append it to the list
                dfs.append(pd.read_parquet(file_path))
            except Exception as e:
                logger.error(f"Error loading {file_path}: {e}")

# Concatenate all DataFrames in the list into a single DataFrame if any were successfully loaded
if dfs:
    df = pd.concat(dfs, ignore_index=True)
    logger.info(f"Total number of rows loaded: {df.shape[0]}")
else:
    logger.error("No data was loaded due to errors in all files.")

# Free up memory by clearing the list
del dfs


logger.info("Calculating total sent and received values for each wallet")
# Calculate total sent and received values for each wallet
sent_values = (
    df.groupby("from")["value"]
    .sum()
    .reset_index()
    .rename(columns={"from": "wallet", "value": "total_sent"})
)
received_values = (
    df.groupby("to")["value"]
    .sum()
    .reset_index()
    .rename(columns={"to": "wallet", "value": "total_received"})
)

logger.info("Calculating the number of transactions for each wallet")
# Calculate the number of transactions for each wallet
sent_count = (
    df.groupby("from")["value"]
    .count()
    .reset_index()
    .rename(columns={"from": "wallet", "value": "sent_count"})
)
received_count = (
    df.groupby("to")["value"]
    .count()
    .reset_index()
    .rename(columns={"to": "wallet", "value": "received_count"})
)

logger.info("Merging sent, received values, and counts into a single DataFrame")
# Merge the sent, received values, and counts into a single DataFrame
wallets = pd.merge(sent_values, received_values, on="wallet", how="outer").fillna(0)
wallets = pd.merge(wallets, sent_count, on="wallet", how="outer").fillna(0)
wallets = pd.merge(wallets, received_count, on="wallet", how="outer").fillna(0)

logger.info("Calculating additional features")
# Calculate additional features
# Convert timestamp to datetime
df["timestamp"] = pd.to_datetime(df["timestamp"])

logger.info("Calculating average time between transactions for each wallet")
# Calculate average time between transactions for each wallet
time_diff = df.groupby("from")["timestamp"].diff().dt.total_seconds().dropna()
avg_time_diff = (
    time_diff.groupby(df["from"])
    .mean()
    .reset_index()
    .rename(columns={"from": "wallet", "timestamp": "avg_time_between_sent"})
)
wallets = pd.merge(wallets, avg_time_diff, on="wallet", how="left").fillna(0)

logger.info("Computing net flow of funds")
# Compute the net flow of funds
wallets["net_flow"] = wallets["total_received"] - wallets["total_sent"]

logger.info("Measuring variance in transaction sizes sent and received")
# Measure the variance in transaction sizes sent and received
sent_variance = (
    df.groupby("from")["value"]
    .var()
    .reset_index()
    .rename(columns={"from": "wallet", "value": "sent_variance"})
)
received_variance = (
    df.groupby("to")["value"]
    .var()
    .reset_index()
    .rename(columns={"to": "wallet", "value": "received_variance"})
)
wallets = pd.merge(wallets, sent_variance, on="wallet", how="left").fillna(0)
wallets = pd.merge(wallets, received_variance, on="wallet", how="left").fillna(0)
wallets = wallets[(wallets["total_sent"] + wallets["total_received"]) >= 1e6]

logger.info("Calculating total transaction count (sent + received)")
# Calculate the total transaction count (sent + received) for sizing the circles
wallets["transaction_count"] = wallets["sent_count"] + wallets["received_count"]

logger.info("Preparing features for clustering")
# Prepare features for clustering
features = wallets[
    ["total_sent", "total_received", "net_flow", "sent_variance", "received_variance"]
]

logger.info("Normalizing features")
# Normalize the features using StandardScaler
scaler = StandardScaler()
scaled_features = scaler.fit_transform(features)

logger.info(
    "Determining the optimal number of clusters using the Elbow Method and Silhouette Score"
)
# Determine the optimal number of clusters using the Elbow Method and Silhouette Score
inertia = []
silhouette_scores = []
K = range(2, 11)  # Testing for 2 to 10 clusters

for k in K:
    kmeans = KMeans(n_clusters=k, random_state=42)
    kmeans.fit(scaled_features)
    inertia.append(kmeans.inertia_)
    silhouette_scores.append(silhouette_score(scaled_features, kmeans.labels_))

logger.info("Plotting Elbow Method graph and Silhouette Scores")
# Plotting the Elbow Method graph and Silhouette Scores
plt.figure(figsize=(18, 8))

# Elbow Method Plot
plt.subplot(1, 2, 1)
plt.plot(K, inertia, "bo-")
plt.xlabel("Number of clusters")
plt.ylabel("Inertia (Sum of squared distances)")
plt.title("Elbow Method")

# Silhouette Scores Plot
plt.subplot(1, 2, 2)
plt.plot(K, silhouette_scores, "ro-")
plt.xlabel("Number of clusters")
plt.ylabel("Silhouette Score")
plt.title("Silhouette Scores for Different k")

plt.tight_layout()
plt.show()

# Choose the optimal k (e.g., based on the highest silhouette score)
optimal_k = (
    silhouette_scores.index(max(silhouette_scores)) + 2
)  # Adding 2 because the index starts from 2 clusters
logger.info(f"Optimal number of clusters determined: {optimal_k}")

logger.info("Applying K-Means clustering using the optimal number of clusters")
# Apply K-Means clustering using the optimal number of clusters
kmeans = KMeans(n_clusters=optimal_k, random_state=42)
wallets["cluster"] = kmeans.fit_predict(scaled_features)

logger.info("Performing weighted clustering")
# Weighted Clustering
weighted_features = wallets[["total_sent", "total_received"]].copy()
weighted_features["total_sent"] *= wallets["transaction_count"]
weighted_features["total_received"] *= wallets["transaction_count"]

# Normalize the weighted features
scaled_weighted_features = scaler.fit_transform(weighted_features)

# Apply K-Means with weighted features
kmeans_weighted = KMeans(n_clusters=optimal_k, random_state=42)
wallets["weighted_cluster"] = kmeans_weighted.fit_predict(scaled_weighted_features)


logger.info("Plotting pairplot of wallet features by cluster")
# Visualization Enhancements
# Pairplot
sns.pairplot(
    wallets[
        ["total_sent", "total_received", "sent_count", "received_count", "cluster"]
    ],
    hue="cluster",
)
plt.suptitle("Pairplot of Wallet Features by Cluster", y=1.02)
plt.show()

logger.info("Plotting heatmap of feature correlations")
# Exclude the 'wallet' and other non-numeric columns before calculating correlations
numeric_columns = wallets.select_dtypes(include=[np.number])

# Now plot the heatmap using only numeric data
plt.figure(figsize=(10, 8))
sns.heatmap(numeric_columns.corr(), annot=True, cmap="coolwarm", linewidths=0.5)
plt.title("Heatmap of Feature Correlations")
plt.show()

logger.info("3D Visualization of Wallet Clusters")
# 3D Visualization
fig = plt.figure(figsize=(10, 8))
ax = fig.add_subplot(111, projection="3d")
ax.scatter(
    wallets["total_sent"],
    wallets["total_received"],
    wallets["net_flow"],
    c=wallets["cluster"],
    cmap="viridis",
    s=50,
)
ax.set_xlabel("Total Sent Value")
ax.set_ylabel("Total Received Value")
ax.set_zlabel("Net Flow")
plt.title("3D Visualization of Wallet Clusters")
plt.show()


# Define a function to format the numbers on the axes
def human_format(x, pos):
    if x >= 1e9:
        return f"{x*1e-9:.1f}B"
    elif x >= 1e6:
        return f"{x*1e-6:.1f}M"
    elif x >= 1e3:
        return f"{x*1e-3:.1f}K"
    else:
        return f"{x:.0f}"


logger.info(
    "Visualizing clusters with real numbers and circle sizes based on the number of transactions"
)
# Visualize the clusters with real numbers and circle sizes based on the number of transactions
plt.figure(figsize=(14, 10))

sns.scatterplot(
    data=wallets,
    x="total_sent",
    y="total_received",
    hue="cluster",
    size="transaction_count",
    sizes=(100, 2000),  # Adjusted circle size range
    palette="viridis",
    alpha=0.7,
)

plt.title("Wallet Clustering Based on Transaction Behavior", fontsize=16)
plt.xlabel("Total Sent Value", fontsize=14)
plt.ylabel("Total Received Value", fontsize=14)

# Apply the human-readable formatting
formatter = FuncFormatter(human_format)
plt.gca().xaxis.set_major_formatter(formatter)
plt.gca().yaxis.set_major_formatter(formatter)

plt.legend(
    title="Cluster", loc="upper left", bbox_to_anchor=(1, 1), fontsize=12
)  # Moved legend to the outside
plt.show()

import matplotlib.pyplot as plt
import seaborn as sns

# Set up the plotting style
sns.set(style="whitegrid")


# Function to create bar plots for top 10 wallets
def plot_top_10(df, column, title, xlabel, ylabel, color_palette="viridis"):
    plt.figure(figsize=(12, 6))
    sns.barplot(x=column, y="wallet", data=df, palette=color_palette)
    plt.title(title, fontsize=16)
    plt.xlabel(xlabel, fontsize=14)
    plt.ylabel(ylabel, fontsize=14)
    plt.show()


# 1. Top 10 Wallets by Net Flow (Positive)
top_net_flow_positive = wallets.sort_values(by="net_flow", ascending=False).head(10)
plot_top_10(
    top_net_flow_positive,
    "net_flow",
    "Top 10 Wallets by Net Flow (Positive)",
    "Net Flow",
    "Wallet",
)

# 2. Top 10 Wallets by Net Flow (Negative)
top_net_flow_negative = wallets.sort_values(by="net_flow", ascending=True).head(10)
plot_top_10(
    top_net_flow_negative,
    "net_flow",
    "Top 10 Wallets by Net Flow (Negative)",
    "Net Flow",
    "Wallet",
)

# 3. Top 10 Wallets by Total Transaction Count
top_transaction_count = wallets.sort_values(
    by="transaction_count", ascending=False
).head(10)
plot_top_10(
    top_transaction_count,
    "transaction_count",
    "Top 10 Wallets by Total Transaction Count",
    "Transaction Count",
    "Wallet",
)

# 4. Top 10 Wallets by Total Sent and Received Combined
wallets["total_transactions_value"] = wallets["total_sent"] + wallets["total_received"]
top_total_transactions_value = wallets.sort_values(
    by="total_transactions_value", ascending=False
).head(10)
plot_top_10(
    top_total_transactions_value,
    "total_transactions_value",
    "Top 10 Wallets by Total Sent and Received",
    "Total Transactions Value",
    "Wallet",
)

# 5. Top 10 Wallets by Weighted Cluster Assignment
top_weighted_cluster = (
    wallets.groupby("weighted_cluster")
    .apply(lambda x: x.nlargest(10, "transaction_count"))
    .reset_index(drop=True)
)
plot_top_10(
    top_weighted_cluster,
    "transaction_count",
    "Top 10 Wallets by Weighted Cluster Assignment",
    "Transaction Count",
    "Wallet",
)

# 6. Comparison of Sent vs. Received for Top 10 Wallets by Transaction Count
plt.figure(figsize=(14, 7))
top_senders_receivers = wallets.sort_values(
    by="transaction_count", ascending=False
).head(10)
sns.barplot(
    x="wallet",
    y="total_sent",
    data=top_senders_receivers,
    label="Total Sent",
    color="blue",
    alpha=0.7,
)
sns.barplot(
    x="wallet",
    y="total_received",
    data=top_senders_receivers,
    label="Total Received",
    color="orange",
    alpha=0.7,
)
plt.title("Top 10 Wallets by Transaction Count: Sent vs. Received", fontsize=16)
plt.xlabel("Wallet", fontsize=14)
plt.ylabel("Transaction Value", fontsize=14)
plt.xticks(rotation=45)
plt.legend(title="Transaction Type")
plt.show()
