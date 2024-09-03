from dune_client.client import DuneClient
from Const import const

class DuneData:
    def __init__(self):
        self.dune = DuneClient(const.ENV['DUNE_KEY'])

    def getResultByQueryId(self, query_id):
        return self.dune.get_latest_result(query_id)