import pandas as pd

from src.libs.utils.pandas.visualization.advanced_plots import AdvancedPlots
from src.libs.utils.pandas.visualization.plot_controller import PlotController

# Sample DataFrame for testing
data = {'X': [1, 2, 3, 4, 5], 'Y1': [10, 20, 30, 40, 50], 'Y2': [5, 15, 25, 35, 45]}
df = pd.DataFrame(data)

# Initialize Plot Controller
plot_controller = PlotController(df)

# Plot using Matplotlib
plot_controller.plot(plot_type='histogram', column='Y1', engine='matplotlib')

# Plot using Seaborn
plot_controller.plot(plot_type='scatter', x='X', y='Y1', engine='seaborn')

# Plot using Plotly
plot_controller.plot(plot_type='line', x='X', y='Y1', engine='plotly')

# Advanced Plotting: Multi-axis plot
adv_plots = AdvancedPlots(df)
adv_plots.multi_axis_plot(x='X', y1='Y1', y2='Y2')
