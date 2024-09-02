from dune_client.client import DuneClient

class DuneData:
    def __init__(self, api_key):
        self.dune = DuneClient(api_key)

    def getResultByQueryId(self, query_id):
        return self.dune.get_latest_result(query_id)