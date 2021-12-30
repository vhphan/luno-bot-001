import os

from binance import Client
from dotenv import load_dotenv

from brokers.broker import Broker

load_dotenv()


class BinanceBroker(Broker):
    BINANCE_API_KEY = os.getenv('BINANCE_API_KEY')
    BINANCE_SECRET_KEY = os.getenv('BINANCE_SECRET_KEY')

    def __init__(self):
        super().__init__(host=None, port=None)
        self.client = Client(self.BINANCE_API_KEY, self.BINANCE_SECRET_KEY)
