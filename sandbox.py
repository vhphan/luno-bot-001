from dotenv import load_dotenv
from luno_python.client import Client
import os

load_dotenv()
LUNO_API_KEY = os.getenv('LUNO_API_KEY')
LUNO_SECRET_KEY = os.getenv('LUNO_SECRET_KEY')

c = Client(api_key_id=LUNO_API_KEY, api_key_secret=LUNO_SECRET_KEY)
try:
    res = c.get_ticker(pair='XBTMYR')
    print(res)
except Exception as e:
    print(e)
