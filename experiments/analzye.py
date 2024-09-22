import pandas as pd
import matplotlib.pyplot as plt
import statsmodels.api as sm
from scipy import stats

# Load the CSV file
df = pd.read_csv("benchmark_results.csv")

# Basic statistics
print("Basic Statistics:")
print(df.describe())

# Mean Total Time by Download Method
download_group = df.groupby("download_method")["total_time"].mean().reset_index()

# Plot the mean total time by download method
plt.figure(figsize=(10, 6))
plt.bar(
    download_group["download_method"], download_group["total_time"], color="skyblue"
)
plt.title("Mean Total Time by Download Method")
plt.xlabel("Download Method")
plt.ylabel("Mean Total Time (s)")
plt.xticks(rotation=45)
plt.show()

# Mean Total Time by Process Method
process_group = df.groupby("process_method")["total_time"].mean().reset_index()

# Plot the mean total time by process method
plt.figure(figsize=(10, 6))
plt.bar(
    process_group["process_method"], process_group["total_time"], color="lightgreen"
)
plt.title("Mean Total Time by Process Method")
plt.xlabel("Process Method")
plt.ylabel("Mean Total Time (s)")
plt.xticks(rotation=45)
plt.show()

# Mean Total Time by Upload Method
upload_group = df.groupby("upload_method")["total_time"].mean().reset_index()

# Plot the mean total time by upload method
plt.figure(figsize=(10, 6))
plt.bar(upload_group["upload_method"], upload_group["total_time"], color="salmon")
plt.title("Mean Total Time by Upload Method")
plt.xlabel("Upload Method")
plt.ylabel("Mean Total Time (s)")
plt.xticks(rotation=45)
plt.show()

# Simplified Trend Analysis
plt.figure(figsize=(12, 8))
colors = ["b", "g", "r", "c", "m"]
for i, method in enumerate(df["download_method"].unique()):
    subset = df[df["download_method"] == method].reset_index()
    plt.plot(subset.index, subset["total_time"], color=colors[i], label=method)
plt.title("Trend Analysis of Total Time by Download Method")
plt.xlabel("Index")
plt.ylabel("Total Time (s)")
plt.legend()
plt.grid(True)
plt.show()

# Detailed Statistical Analysis
# Filter out non-numeric columns for correlation matrix
numeric_df = df.select_dtypes(include=["float64", "int64"])

# Correlation Matrix
correlation_matrix = numeric_df.corr()
print("\nCorrelation Matrix:")
print(correlation_matrix)

# Heatmap of Correlation Matrix
plt.figure(figsize=(10, 8))
plt.imshow(correlation_matrix, cmap="coolwarm", interpolation="nearest")
plt.colorbar()
plt.xticks(
    range(len(correlation_matrix.columns)), correlation_matrix.columns, rotation=45
)
plt.yticks(range(len(correlation_matrix.columns)), correlation_matrix.columns)
plt.title("Correlation Matrix Heatmap")
plt.show()

# Regression Analysis: Total Time vs. Rows Processed
X = df["rows_processed"]
y = df["total_time"]
X = sm.add_constant(X)  # Adds a constant term to the predictor

model = sm.OLS(y, X).fit()
predictions = model.predict(X)

print("\nRegression Analysis Summary:")
print(model.summary())

# Scatter Plot with Regression Line
plt.figure(figsize=(10, 6))
plt.scatter(df["rows_processed"], df["total_time"], alpha=0.5)
plt.plot(df["rows_processed"], predictions, color="red")
plt.title("Regression Analysis: Total Time vs. Rows Processed")
plt.xlabel("Rows Processed")
plt.ylabel("Total Time (s)")
plt.show()

# Hypothesis Testing: T-test between two methods
download_sync = df[df["download_method"] == "download_sync"]["total_time"]
download_threaded = df[df["download_method"] == "download_threaded"]["total_time"]

t_stat, p_value = stats.ttest_ind(download_sync, download_threaded)
print(
    f"\nT-test between 'download_sync' and 'download_threaded': T-statistic = {t_stat}, P-value = {p_value}"
)
