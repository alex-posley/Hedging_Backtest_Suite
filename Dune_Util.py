from dune_client.client import DuneClient

class DuneData:
    def __init__(self, api_key):
        self.dune = DuneClient(api_key)
        
    query_result = dune.get_latest_result(3379698)