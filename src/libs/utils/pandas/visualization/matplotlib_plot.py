import matplotlib.pyplot as plt


class MatplotlibPlot:
    """
    Handles plotting using Matplotlib.
    """

    def __init__(self, df):
        self.df = df

    def plot(self, plot_type, column=None, x=None, y=None):
        """
        Creates a plot based on the type of plot requested.
        :param plot_type: Type of plot (e.g., 'histogram', 'scatter', 'line').
        :param column: The column to plot for single column plots (e.g., histogram).
        :param x: The x-axis column for scatter or line plots.
        :param y: The y-axis column for scatter or line plots.
        :return: Plot
        """
        if plot_type == 'histogram' and column:
            self.df[column].hist()
        elif plot_type == 'scatter' and x and y:
            self.df.plot.scatter(x=x, y=y)
        elif plot_type == 'line' and x and y:
            self.df.plot.line(x=x, y=y)
        else:
            raise ValueError(f"Unsupported plot type: {plot_type}")

        plt.show()
