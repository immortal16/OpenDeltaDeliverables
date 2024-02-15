import time
import ccxt
import requests

from datetime import datetime, timezone
from pandas import DataFrame, to_datetime, concat, merge


class DataCollector:
    def __init__(self, coinglass_api_key):
        self.baseURL = 'https://open-api-v3.coinglass.com/api/futures/'
        self.headers = {
            'accept': 'application/json',
            'CG-API-KEY': coinglass_api_key
        }
        self.validation_data = self.get_validation_data()


    def get_validation_data(self):
        return requests.get(
            self.baseURL + 'supported-exchange-pairs',
            headers=self.headers
        ).json()['data']


    def validate(self, exchange, symbol):
        if exchange not in self.validation_data:
            return False

        for instrument in self.validation_data[exchange]:
            if instrument['instrumentId'] == symbol:
                return True

        return False


    def get_symbol_tickers_coin_glass(self, exchange, symbol):
        for ticker in self.validation_data[exchange]:
            if symbol in ticker['instrumentId']:
                print(ticker)


    def get_symbol_tickers_ccxt(self, exchange_instance, symbol):
        tickers = exchange_instance.fetchMarkets()
        for ticker in tickers:
            if symbol in ticker['symbol']:
                print(ticker)


    @staticmethod
    def convert_interval(interval):
        match interval[-1]:
            case 'm':
                return int(interval[:-1]) * 60
            case 'h':
                return int(interval[:-1]) * 60 * 60
            case 'd':
                return 24 * 60 * 60
            case 'w':
                return 7 * 24 * 60 * 60
            case _:
                raise ValueError('invalid interval')


    def fetch_OHLCV(self, exchange_instance, symbol, interval, start, end, additional_params={}, hold=5):
        assert exchange_instance.has['fetchOHLCV'], 'exchange fetch OHLCV method is not supported in ccxt'

        start_dt = datetime.strptime(start, '%d.%m.%Y').replace(tzinfo=timezone.utc)
        end_dt = datetime.strptime(end, '%d.%m.%Y').replace(tzinfo=timezone.utc)

        start_timestamp = int(start_dt.timestamp() * 1e3)
        end_timestamp = int(end_dt.timestamp() * 1e3)
        
        multiplier = int(1e3 * self.convert_interval(interval))

        ohlcvs = []

        while start_timestamp <= end_timestamp:

            try:
                ohlcv = exchange_instance.fetch_ohlcv(
                    symbol=symbol,
                    timeframe=interval,
                    since=start_timestamp,
                    params=additional_params
                )

                ohlcvs += ohlcv
                start_timestamp += int(len(ohlcv) * multiplier) if len(ohlcv) else multiplier

            except (ccxt.ExchangeError, ccxt.AuthenticationError, ccxt.ExchangeNotAvailable, ccxt.RequestTimeout) as error:
                print('Got an error', type(error).__name__, error.args, ', retrying in', hold, 'seconds...')
                time.sleep(hold)

        ret = DataFrame(ohlcvs, columns=['timestamp', 'Open', 'High', 'Low', 'Close', 'Volume'])
        ret = ret[ret['timestamp'] < end_timestamp]
        ret['timestamp'] = to_datetime(ret['timestamp'], unit='ms')
        if interval[-1] in 'dwm':
            ret['timestamp'] = ret['timestamp'].dt.floor('D')
        ret = ret.reset_index(drop=True)
        
        return ret

    
    def get_ohlcv(self, exchange, symbol, interval, start, end, futures=False):
        match exchange:
            case 'OKX':
                return self.fetch_OHLCV(ccxt.okx(), symbol, interval, start, end)
            case 'Kraken':
                if futures:
                    return self.fetch_OHLCV(ccxt.krakenfutures(), symbol, interval, start, end)
                else:
                    return self.fetch_OHLCV(ccxt.kraken(), symbol, interval, start, end)
            case 'Huobi':
                return self.fetch_OHLCV(ccxt.huobi(), symbol, interval, start, end)
            case 'Deribit':
                return self.fetch_OHLCV(ccxt.deribit(), symbol, interval, start, end)
            case 'Bybit':
                return self.fetch_OHLCV(ccxt.bybit(), symbol, interval, start, end)
            case 'Binance':
                if futures:
                    return self.fetch_OHLCV(ccxt.binancecoinm(), symbol, interval, start, end)
                else:
                    return self.fetch_OHLCV(ccxt.binance(), symbol, interval, start, end)
            case 'Bitget':
                return self.fetch_OHLCV(ccxt.bitget(), symbol, interval, start, end)
            case 'Bitmex':
                return self.fetch_OHLCV(ccxt.bitmex(), symbol, interval, start, end)
            case _:
                raise ValueError('OI/FR data would not be available for this exchange')


    def get_oi_ohlc(self, exchange, symbol, interval, start, end):
        assert self.validate(exchange, symbol), 'unsupported exchange or symbol'

        start_dt = datetime.strptime(start, '%d.%m.%Y').replace(tzinfo=timezone.utc)
        end_dt = datetime.strptime(end, '%d.%m.%Y').replace(tzinfo=timezone.utc)

        start_timestamp = int(start_dt.timestamp())
        end_timestamp = int(end_dt.timestamp())

        ret = DataFrame(columns=['timestamp', 'OI Open', 'OI High', 'OI Low', 'OI Close'])

        increment = 4500 * self.convert_interval(interval)

        while start_timestamp <= end_timestamp:

            data = requests.get(
                self.baseURL + 'openInterest/ohlc-history',
                headers=self.headers,
                params={
                    'exchange': exchange,
                    'symbol': symbol,
                    'interval': interval,
                    'startTime': start_timestamp,
                    'endTime': end_timestamp,
                    'limit': 4500
                }
            ).json()['data']

            df = DataFrame(data)
            df['timestamp'] = to_datetime(df['t'], unit='s')
            df = df.rename(columns={'o': 'OI Open', 'h': 'OI High', 'l': 'OI Low', 'c': 'OI Close'})
            df = df.drop(columns=['t'])

            numeric_columns = ['OI Open', 'OI High', 'OI Low', 'OI Close']
            df[numeric_columns] = df[numeric_columns].astype(float)

            ret = concat([ret, df])

            start_timestamp += increment

        ret = ret.reset_index(drop=True)
        return ret


    def get_fr_ohlc(self, exchange, symbol, interval, start, end):
        assert self.validate(exchange, symbol), 'unsupported exchange or symbol'

        start_dt = datetime.strptime(start, '%d.%m.%Y').replace(tzinfo=timezone.utc)
        end_dt = datetime.strptime(end, '%d.%m.%Y').replace(tzinfo=timezone.utc)

        start_timestamp = int(start_dt.timestamp())
        end_timestamp = int(end_dt.timestamp())

        ret = DataFrame(columns=['timestamp', 'FR Open', 'FR High', 'FR Low', 'FR Close'])

        increment = 4500 * self.convert_interval(interval)

        while start_timestamp <= end_timestamp:

            data = requests.get(
                self.baseURL + 'fundingRate/ohlc-history',
                headers=self.headers,
                params={
                    'exchange': exchange,
                    'symbol': symbol,
                    'interval': interval,
                    'startTime': start_timestamp,
                    'endTime': end_timestamp,
                    'limit': 4500
                }
            ).json()['data']

            df = DataFrame(data)
            df['timestamp'] = to_datetime(df['t'], unit='s')
            df = df.rename(columns={'o': 'FR Open', 'h': 'FR High', 'l': 'FR Low', 'c': 'FR Close'})
            df = df.drop(columns=['t'])

            numeric_columns = ['FR Open', 'FR High', 'FR Low', 'FR Close']
            df[numeric_columns] = df[numeric_columns].astype(float)

            ret = concat([ret, df])

            start_timestamp += increment

        ret = ret.reset_index(drop=True)
        return ret


    def get_all(self, exchange, symbol_ccxt, symbol_coinglass, interval, start, end, futures=False):
        oi_data = self.get_oi_ohlc(exchange, symbol_coinglass, interval, start, end)
        fr_data = self.get_fr_ohlc(exchange, symbol_coinglass, interval, start, end)
        ohlcv_data = self.get_ohlcv(exchange, symbol_ccxt, interval, start, end, futures)
        
        all_data = merge(ohlcv_data, oi_data, on='timestamp', how='inner')
        all_data = merge(all_data, fr_data, on='timestamp', how='inner')
        all_data = all_data.dropna()
        all_data = all_data.set_index('timestamp')
        return all_data