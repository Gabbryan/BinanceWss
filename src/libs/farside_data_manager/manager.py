class FarsideDataManager:
    def __init__(self):
        self.ingestors = []

    def register_ingestor(self, ingestor):
        self.ingestors.append(ingestor)

    def run(self):
        for ingestor in self.ingestors:
            ingestor.fetch_data()
            ingestor.process_data()
