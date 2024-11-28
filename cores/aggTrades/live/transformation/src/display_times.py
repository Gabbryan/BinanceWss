import json
import numpy as np
import matplotlib.pyplot as plt


def load_log_data(file_path):
    with open(file_path, "r") as file:
        logs = json.load(file)
    return logs


def compute_statistics(logs):
    times = np.array([log["elapsed_time"] for log in logs])
    trade_counts = np.array([log["trade_count"] for log in logs])
    stats = {
        "mean_time": np.mean(times),
        "median_time": np.median(times),
        "std_dev_time": np.std(times),
        "min_time": np.min(times),
        "max_time": np.max(times),
        "total_trades": np.sum(trade_counts),
        "mean_trades_per_call": np.mean(trade_counts),
    }
    return stats


def plot_histogram(times, title, xlabel):
    plt.figure(figsize=(10, 6))
    plt.hist(times, bins=20, color="blue", alpha=0.7)
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel("Frequency")
    plt.grid(True)
    plt.show()


def plot_time_series(times, title, xlabel, ylabel):
    plt.figure(figsize=(10, 6))
    plt.plot(times, marker="o", linestyle="-")
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.grid(True)
    plt.show()


def main():
    file_path = "time_logs.json"
    logs = load_log_data(file_path)

    if not logs:
        print("No data found in the file.")
        return

    # Extract elapsed times for plotting and stats
    elapsed_times = [log["elapsed_time"] for log in logs]
    trade_counts = [log["trade_count"] for log in logs]

    # Compute statistics
    stats = compute_statistics(logs)
    for key, value in stats.items():
        print(f"{key.replace('_', ' ').capitalize()}: {value:.2f}")

    # Plot histogram of elapsed times
    plot_histogram(
        elapsed_times, "Histogram of API Call Elapsed Times", "Time (seconds)"
    )

    # Plot histogram of trades per call
    plot_histogram(trade_counts, "Histogram of Trades per API Call", "Number of Trades")

    # Plot time series of elapsed times
    plot_time_series(
        elapsed_times,
        "Time Series of API Call Durations",
        "API Call Index",
        "Elapsed Time (seconds)",
    )

    # Plot time series of trades per call
    plot_time_series(
        trade_counts,
        "Time Series of Trades per API Call",
        "API Call Index",
        "Number of Trades",
    )


if __name__ == "__main__":
    main()
