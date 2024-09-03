import requests
import json
from Const import const
from typing import List

class AccountData:

    def __init__(self, accountAddress: str) -> None:
        self.jsonRPCUrl = const.ENV['SOL_JSONRPC_URL']
        self.publicKey = accountAddress
    
    def sendJsonRPCRequest(self, method: str, params: List):
        data = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params
        }
        response = requests.post(self.jsonRPCUrl, headers={"Content-Type": "application/json"}, data=json.dumps(data))
        return response.json()
    
    def getHistoricalData(self, slot: int):
        return self.sendJsonRPCRequest("get_account_info", [self.publicKey, {
            "encoding": "base64",
            "commitment": "finalized",
            "minContextSlot": slot
        }])

        
        