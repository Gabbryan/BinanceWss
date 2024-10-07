from src.commons.logs.logging_controller import LoggingController


class FileManager:
    def __init__(self, file_system):
        self.fs = file_system
        self.logger = LoggingController("FileManager")

    def read_file(self, path):
        """ Generic method to read a file. Must be implemented in subclass """
        raise NotImplementedError("This method should be overridden by subclasses")

    def process_file(self, path, params):
        """ Method to process files, can be extended by specific file managers """
        try:
            with self.fs.open(path) as f:
                df = self.read_file(f)
            return self.process_data(df, params)
        except Exception as e:
            self.logger.log_error(f"Error processing file {path}: {e}")
            return None

    def process_data(self, df, params):
        """ Placeholder for data processing logic, to be customized by subclasses """
        raise NotImplementedError("This method should be overridden by subclasses")
