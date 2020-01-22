#%%
# import packages
import json, time, requests
import base64, hmac, hashlib, sqlite3, asyncio
import dateutil.parser as du
from requests.auth import AuthBase


config = {

    'PAIR': 'BTC-EUR',
    'CANDLE_DURATION': 600,  
    'EXCHANGE_NAME': 'Coinbase',
    'API_PUB_URL': 'https://api.coinbase.com/v2/',
    'API_PRO_URL': 'https://api.pro.coinbase.com/',
    'API_KEY': '12345678',
    'API_SECRET': '12345678', 
    'API_PASS': '12345678' 
}

# set parameters
pub_url = config['API_PUB_URL']
pro_url = config['API_PRO_URL']
exchange_name = config['EXCHANGE_NAME']
pair = config['PAIR']
candle_duration = config['CANDLE_DURATION']

# create custom authentication
class Auth(AuthBase):
    def __init__(self, api_key, secret_key, passphrase):
        self.api_key = api_key
        self.secret_key = secret_key
        self.passphrase = passphrase

    def __call__(self, request):
        timestamp = str(time.time())
        message = timestamp + request.method + request.path_url + (request.body or b'').decode()
        hmac_key = base64.b64decode(self.secret_key)
        signature = hmac.new(hmac_key, message.encode(), hashlib.sha256)
        signature_b64 = base64.b64encode(signature.digest()).decode()

        request.headers.update({
            'CB-ACCESS-SIGN': signature_b64,
            'CB-ACCESS-TIMESTAMP': timestamp,
            'CB-ACCESS-KEY': self.api_key,
            'CB-ACCESS-PASSPHRASE': self.passphrase,
            'Content-Type': 'application/json'
        })
        return request

auth = Auth(config['API_KEY'], config['API_SECRET'], config['API_PASS'])

# SQL database
connection = sqlite3.connect('test.db')
c = connection.cursor()

c.execute('''CREATE TABLE IF NOT EXISTS last_checks
    (Id INTEGER PRIMARY KEY AUTOINCREMENT, exchange TEXT, trading_pair TEXT, duration TEXT, table_name TEXT, last_check INT,
    startdate INT, last_id INT)''')

candles_table_name = str(exchange_name).replace('-', '_') + '_' + str(pair).replace('-', '_') + '_Candles_'+ str(candle_duration)
table_creation_statement = '''CREATE TABLE IF NOT EXISTS ''' + candles_table_name + \
    '''(Id INTEGER PRIMARY KEY AUTOINCREMENT, date INT, high REAL, low REAL, open REAL, close REAL, volume REAL,
    quotevolume REAL, weightedaverage REAL, sma_7 REAL, ema_7 REAL, sma_30 REAL, ema_30 REAL, sma_200 REAL, ema_200 REAL)'''
c.execute(table_creation_statement)

trades_table_name = str(exchange_name) + '_' + str(pair).replace('-', '_') + '_Trades'
table_creation_statement = '''CREATE TABLE IF NOT EXISTS ''' + trades_table_name + \
    '''(Id INTEGER PRIMARY KEY AUTOINCREMENT, uuid TEXT, traded_btc REAL, price REAL, created_at_int INT, side TEXT)'''
c.execute(table_creation_statement)




def getDepth(direction, pair=pair, api_url=pub_url):
    Dict = {'mid': 'spot', 'bid': 'sell', 'ask': 'buy'}
    x = requests.get(api_url + 'prices/{0}/{1}'.format(pair, Dict[direction])).json()['data']
    timestamp = requests.get(api_url + 'time').json()['data']
    x.update(timestamp)
    return x

def getOrderBook(pair, level=2, api_url=pro_url):

    x = requests.get(api_url + 'products/{0}/book?level={1}'.format(pair, level)).json()
    return x

def refreshDataCandles(pair=pair, duration=candle_duration, cursor=c, table=candles_table_name, api_url=pro_url):
    x = requests.get(api_url + 'products/{0}/candles?granularity{1}'.format(pair, duration)).json()
    last_date = cursor.execute('''SELECT date FROM ''' + table + ''' ORDER BY date DESC LIMIT 1''').fetchone()
    if last_date is None:
        last_date = [-1,]
    if (x[0][0] != last_date[0]):
        
        for r in x:
            cursor.execute('''INSERT INTO ''' + table \
                + '''(date,high,low,open,close,volume,quotevolume,weightedaverage,sma_7,ema_7,sma_30,ema_30,sma_200,ema_200)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                [r[0], r[2], r[1], r[3], r[4], r[5], 0, 0, 0, 0, 0, 0, 0, 0])
        last_id = cursor.lastrowid
        cursor.execute('''INSERT INTO last_checks(exchange,trading_pair,duration,table_name,last_check,startdate,last_id)
            VALUES(?,?,?,?,?,?,?)''',
            [exchange_name, pair, duration, table, int(time.time()), x[-1][0], last_id])

def refreshData(pair=pair, cursor=c, table=trades_table_name, api_url=pro_url):
    
    res = requests.get(api_url + 'products/{0}/trades'.format(pair)).json()
    for r in res:
        cursor.execute('''INSERT INTO ''' + table \
            + '''(uuid,traded_btc,price,created_at_int,side) VALUES (?,?,?,?,?)''',
            [r['trade_id'], r['size'], r['price'], int(du.parse(r['time']).timestamp()), r['side']])
    last_id = cursor.lastrowid

    cursor.execute('''INSERT INTO last_checks(exchange,trading_pair,duration,table_name,last_check,startdate,last_id)
        VALUES(?,?,?,?,?,?,?)''',
        [exchange_name, pair, 0, table, int(time.time()), int(du.parse(res[-1]['time']).timestamp()), last_id])

def createOrder(direction, price, amount, order_type, pair=pair, auth=auth, api_url=pro_url):
    order = {
    'size': amount,
    'price': price,
    'side': direction,
    'product_id': pair,
    'type': order_type
    }
    x = requests.post(api_url + 'orders', json=order, auth=auth).json()
    return x

def cancelOrder(order_id, auth=auth, api_url=pro_url):

    x = requests.post(api_url + 'orders/{}'.format(order_id), auth=auth).json()
    return x



# print price of selected pair
print('\nPair: {}\n'.format(pair))
print('Ask: {0}\nBid: {1}\n'.format(getDepth(direction='ask')['amount'], getDepth(direction='bid')['amount']))

refreshDataCandles()
refreshData()



# limit order
order_res = createOrder(direction='sell', price='1000', amount='0.1', order_type='limit')
canc_res = cancelOrder(order_res)
print('\n\nOrder response: {0}\nCancellation response: {1}'.format(order_res, canc_res))
connection.commit()

# %%
