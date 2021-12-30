import os

import pandas as pd
from dotenv import load_dotenv

from bots.moon_bot import MoonClient
from decorate.my_email import send_eri_mail

load_dotenv()
LUNO_API_KEY = os.getenv('LUNO_API_KEY_TRADE')
LUNO_SECRET_KEY = os.getenv('LUNO_SECRET_KEY_TRADE')
recipient_email = os.getenv('recipient_email')

if __name__ == '__main__':
    c = MoonClient(api_key_id=LUNO_API_KEY, api_key_secret=LUNO_SECRET_KEY)
    balances = c.get_balances()
    df = pd.DataFrame(balances.get('balance'))
    msg = df.to_html()
    send_eri_mail(msg, recipient_email, subject='LUNO balances')
