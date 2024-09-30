import matplotlib.pyplot as plt


class AdvancedPlots:
    """
    Handles advanced plotting like multi-axis plots and subplots.
    """

    def __init__(self, df):
        self.df = df

    def multi_axis_plot(self, x, y1, y2):
        """
        Creates a multi-axis plot with two y-axes.
        :param x: The x-axis column.
        :param y1: The first y-axis column.
        :param y2: The second y-axis column.
        :return: Multi-axis plot
        """
        fig, ax1 = plt.subplots()

        ax2 = ax1.twinx()
        ax1.plot(self.df[x], self.df[y1], 'g-')
        ax2.plot(self.df[x], self.df[y2], 'b-')

        ax1.set_xlabel('X data')
        ax1.set_ylabel(y1, color='g')
        ax2.set_ylabel(y2, color='b')

        plt.show()
