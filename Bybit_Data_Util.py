from typing import Dict, List, Literal
import pandas as pd
from pybit.unified_trading import HTTP, MarketHTTP

class BybitData:

    def __init__(self) -> None:
        self.session: MarketHTTP = HTTP(testnet=False)

    def getPriceRangeByTime(self, startTimeStamp: int, endTimeStamp: int, symbol: str, interval=Literal['1', '3', '5']) -> Dict:
        # timestamp in ms
        result = []
        while startTimeStamp < endTimeStamp:
            stepEndTimeStamp = min(startTimeStamp + 1000 * int(interval) * 60 * 1000, endTimeStamp)
            res: Dict = self.session.get_mark_price_kline(
                symbol=symbol, 
                start=startTimeStamp, 
                end=stepEndTimeStamp, 
                category='linear',
                limit=1000,
                interval=interval
                )
            # print(res)
            result.extend(res['result']['list'])
            startTimeStamp = stepEndTimeStamp
        return result

    def convertKlineDataToDf(self, data: List[List]) -> pd.DataFrame:
        data = [{
            'Timestamp': int(row[0]),
            'Open': float(row[1]),
            'High': float(row[2]),
            'Low': float(row[3]),
            'Close': float(row[4]),
        } for row in data]
        df = pd.DataFrame(data=data)
        df = df.drop_duplicates(keep='last', subset=['Timestamp'])
        return df