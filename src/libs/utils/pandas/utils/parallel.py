import datetime as dt
import multiprocessing as mp
import sys
import time

import pandas as pd


class ParallelPandas:
    """
    A class to handle parallel processing of pandas operations and manage large-scale DataFrames.
    """

    def __init__(self, numThreads=24, mpBatches=1, linMols=True):
        """
        Initialize the class with default settings for parallelization.
        :param numThreads: Number of threads to use for multiprocessing.
        :param mpBatches: Number of batches to use for multiprocessing.
        :param linMols: Whether to use linear molecule partitioning.
        """
        self.numThreads = numThreads
        self.mpBatches = mpBatches
        self.linMols = linMols

    # Helper functions for partitioning data
    def linParts(self, numAtoms, numParts):
        parts = [0]
        delta = numAtoms // numParts
        for i in range(1, numParts):
            parts.append(parts[-1] + delta)
        parts.append(numAtoms)
        return parts

    def nestedParts(self, numAtoms, numParts):
        # For illustration purposes: this is a placeholder for more complex logic
        return self.linParts(numAtoms, numParts)

    # Expand call used for processing the job
    @staticmethod
    def expandCall(kargs):
        func = kargs['func']
        del kargs['func']
        return func(**kargs)

    # Helper to run the jobs sequentially (for debugging)
    @staticmethod
    def processJobs_(jobs):
        out = []
        for job in jobs:
            out_ = ParallelPandas.expandCall(job)
            out.append(out_)
        return out

    # Report progress of the parallel execution
    @staticmethod
    def reportProgress(jobNum, numJobs, time0, task):
        msg = [float(jobNum) / numJobs, (time.time() - time0) / 60.]
        msg.append(msg[1] * (1 / msg[0] - 1))
        timeStamp = str(dt.datetime.fromtimestamp(time.time()))
        msg = (timeStamp + ' ' + str(round(msg[0] * 100, 2)) + '% ' + task +
               ' done after ' + str(round(msg[1], 2)) + ' minutes. Remaining ' +
               str(round(msg[2], 2)) + ' minutes.')
        if jobNum < numJobs:
            sys.stderr.write(msg + '\r')
        else:
            sys.stderr.write(msg + '\n')

    # Function to handle multiprocessing jobs asynchronously
    def processJobs(self, jobs, task=None):
        if task is None:
            task = jobs[0]['func'].__name__

        pool = mp.Pool(processes=self.numThreads)
        outputs, out, time0 = pool.imap_unordered(ParallelPandas.expandCall, jobs), [], time.time()

        # Process asynchronous output, report progress
        for i, out_ in enumerate(outputs, 1):
            out.append(out_)
            self.reportProgress(i, len(jobs), time0, task)

        pool.close()
        pool.join()  # Prevent memory leaks
        return out

    # Main function to parallelize jobs
    def mpPandasObj(self, func, pdObj, **kargs):
        """
        Parallelize jobs, return a DataFrame or Series.
        + func: function to be parallelized. Returns a DataFrame or Series.
        + pdObj[0]: Name of the argument used to pass the molecule.
        + pdObj[1]: List of atoms that will be grouped into molecules.
        + kargs: any other argument needed by func.
        """
        # Determine the partitioning of jobs
        if self.linMols:
            parts = self.linParts(len(pdObj[1]), self.numThreads * self.mpBatches)
        else:
            parts = self.nestedParts(len(pdObj[1]), self.numThreads * self.mpBatches)

        jobs = []
        for i in range(1, len(parts)):
            job = {pdObj[0]: pdObj[1][parts[i - 1]: parts[i]], 'func': func}
            job.update(kargs)
            jobs.append(job)

        # Execute in parallel or single-threaded for debugging
        if self.numThreads == 1:
            out = self.processJobs_(jobs)
        else:
            out = self.processJobs(jobs)

        # Collect the output and return as DataFrame or Series
        if isinstance(out[0], pd.DataFrame):
            df0 = pd.concat(out)
        elif isinstance(out[0], pd.Series):
            df0 = pd.concat(out)
        else:
            return out

        return df0.sort_index()

    # New method to generate large synthetic DataFrame
    # New method to test performance of parallel processing on large DataFrames
    def test_parallel_pandas(self, df, column, func, **kargs):
        """
        Runs parallel operations on a large DataFrame using ParallelPandas.
        This tests the performance and correctness on a dataset with millions of rows.
        :param df: The DataFrame to process.
        :param column: The column to apply the function in parallel.
        :param func: The function to be applied in parallel.
        :param kargs: Additional arguments to pass to the custom function (e.g., factor, threshold, operation).
        :return: Resulting DataFrame or Series.
        """
        # Time the parallel processing
        start_time = time.time()

        # Apply the custom function to the specified column in parallel
        result = self.mpPandasObj(
            func=func,
            pdObj=('molecule', df[column]),  # Parallelize across the specified column
            **kargs  # Pass additional arguments to the function
        )

        # Time the end of the parallel processing
        end_time = time.time()
        print(f"Parallel processing completed in {round(end_time - start_time, 2)} seconds.")

        # Return the result
        return result
