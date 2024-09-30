from src.libs.utils.pandas.visualization.matplotlib_plot import MatplotlibPlot
from src.libs.utils.pandas.visualization.plotly_plot import PlotlyPlot
from src.libs.utils.pandas.visualization.seaborn_plot import SeabornPlot


class PlotController:
    """
    Unified interface for handling plotting operations using different libraries (matplotlib, seaborn, plotly).
    """

    def __init__(self, df):
        self.df = df
        self.plot_engines = {
            'matplotlib': MatplotlibPlot(df),
            'seaborn': SeabornPlot(df),
            'plotly': PlotlyPlot(df)
        }

    def plot(self, plot_type, column=None, x=None, y=None, engine='matplotlib'):
        """
        Delegates plotting to the appropriate plotting engine.
        :param plot_type: Type of plot (e.g., 'histogram', 'scatter', 'line').
        :param column: The column to plot for single column plots.
        :param x: The x-axis column for scatter or line plots.
        :param y: The y-axis column for scatter or line plots.
        :param engine: The plotting engine ('matplotlib', 'seaborn', 'plotly').
        :return: Plot
        """
        if engine in self.plot_engines:
            return self.plot_engines[engine].plot(plot_type, column, x, y)
        else:
            raise ValueError(f"Unsupported plotting engine: {engine}")
