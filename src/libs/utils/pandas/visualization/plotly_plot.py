import plotly.express as px


class PlotlyPlot:
    """
    Handles plotting using Plotly for interactive plots.
    """

    def __init__(self, df):
        self.df = df

    def plot(self, plot_type, column=None, x=None, y=None):
        """
        Creates an interactive plot using Plotly.
        :param plot_type: Type of plot (e.g., 'histogram', 'scatter', 'line').
        :param column: The column to plot for single column plots (e.g., histogram).
        :param x: The x-axis column for scatter or line plots.
        :param y: The y-axis column for scatter or line plots.
        :return: Interactive Plot
        """
        if plot_type == 'histogram' and column:
            fig = px.histogram(self.df, x=column)
        elif plot_type == 'scatter' and x and y:
            fig = px.scatter(self.df, x=x, y=y)
        elif plot_type == 'line' and x and y:
            fig = px.line(self.df, x=x, y=y)
        else:
            raise ValueError(f"Unsupported plot type: {plot_type}")

        fig.show()
