from src.libs.utils.pandas.analytics.advanced_stats import AdvancedStats
from src.libs.utils.pandas.analytics.missing_data import MissingData
from src.libs.utils.pandas.analytics.summary_operations import SummaryOperations
from src.libs.utils.pandas.analytics.time_series_operations import TimeSeriesOperations
from src.libs.utils.pandas.config.config import PandasConfig
from src.libs.utils.pandas.io.io_controller import IOController
from src.libs.utils.pandas.manipulation.advanced_transformations import AdvancedTransformations
from src.libs.utils.pandas.manipulation.column_operations import ColumnOperations
from src.libs.utils.pandas.manipulation.dataframe_operations import DataFrameOperations
from src.libs.utils.pandas.manipulation.date_operations import DateOperations
from src.libs.utils.pandas.manipulation.row_operations import RowOperations
from src.libs.utils.pandas.utils.dataframe_generator import DataFrameGenerator
from src.libs.utils.pandas.utils.parallel import ParallelPandas
from src.libs.utils.pandas.utils.validators import Validators
from src.libs.utils.pandas.visualization.matplotlib_plot import MatplotlibPlot


class PandasManager:
    """
    A generic class that acts as a top-level interface for various pandas operations.
    It provides direct access to utility classes such as DataFrameGenerator, IOController, etc.
    """

    def __init__(self, df=None, config_base_path=None, num_threads=24, mp_batches=1, lin_mols=True):
        """
        Initialize the PandasManager and set up all utility classes.
        :param config_based_path: Optional base path for configuration files.
        """
        self.df = df
        self.config_base_path = config_base_path

        # Initialize utilities as attributes
        self.DataFrameGenerator = DataFrameGenerator()
        self.IOController = IOController()
        self.Config = PandasConfig(config_base_path)

        # Initialize manipulation operations
        self.RowOperations = RowOperations(self.df)
        self.ColumnOperations = ColumnOperations(self.df)
        self.DataFrameOperations = DataFrameOperations(self.df)
        self.AdvancedTransformations = AdvancedTransformations(self.df)
        self.DateOperations = DateOperations(self.df)

        # Initialize analytics operations
        self.MissingData = MissingData(self.df)
        self.SummaryOperations = SummaryOperations(self.df)
        self.TimeSeriesOperations = TimeSeriesOperations(self.df)
        self.AdvancedStats = AdvancedStats(self.df)

        # Initialize additional utilities
        self.ParallelPandas = ParallelPandas(num_threads, mp_batches, lin_mols)
        self.Validators = Validators(self.df)

        # Initialize visualization
        self.MatplotlibPlot = MatplotlibPlot
