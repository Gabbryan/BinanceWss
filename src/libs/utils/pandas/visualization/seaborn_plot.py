import matplotlib.pyplot as plt
import seaborn as sns


class SeabornPlot:
    """
    Handles plotting using Seaborn.
    """

    def __init__(self, df):
        self.df = df

    def plot(self, plot_type, column=None, x=None, y=None):
        """
        Creates a plot using Seaborn based on the type of plot requested.
        :param plot_type: Type of plot (e.g., 'histogram', 'scatter', 'line').
        :param column: The column to plot for single column plots (e.g., histogram).
        :param x: The x-axis column for scatter or line plots.
        :param y: The y-axis column for scatter or line plots.
        :return: Plot
        """
        if plot_type == 'histogram' and column:
            sns.histplot(self.df[column])
        elif plot_type == 'scatter' and x and y:
            sns.scatterplot(x=self.df[x], y=self.df[y])
        elif plot_type == 'line' and x and y:
            sns.lineplot(x=self.df[x], y=self.df[y])
        else:
            raise ValueError(f"Unsupported plot type: {plot_type}")

        plt.show()
