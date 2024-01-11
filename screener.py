from tda.auth import easy_client
from tda.client import Client
from tda.streaming import StreamClient
import pandas as pd
import json
import smtplib
import ssl
from email.message import EmailMessage
import datetime
from time import sleep
import winsound


class TDAStockScanner:

    def __init__(self, client_parameters, scan_parameters=None, notification_parameters=None, watchlist=[],
                 max_watchlist_size=300, alert_sound=None):

        now = datetime.datetime.now()
        self.scan_start = now
        self.day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        self.market_reset = now.replace(hour=7, minute=28, second=0, microsecond=0)
        self.market_open = now.replace(hour=9, minute=30, second=0, microsecond=0)
        self.market_close = now.replace(hour=16, minute=0, second=0, microsecond=0)

        self.day_start_timestamp = int(datetime.datetime.timestamp(self.day_start)) * 1000
        self.market_reset_timestamp = int(datetime.datetime.timestamp(self.market_reset)) * 1000
        self.market_open_timestamp = int(datetime.datetime.timestamp(self.market_open)) * 1000
        self.market_close_timestamp = int(datetime.datetime.timestamp(self.market_close)) * 1000

        self.market_period = self.get_market_period()

        self.client_parameters = client_parameters

        self.client = easy_client(**client_parameters['client_parameters'])

        if 'stream_client_parameters' in client_parameters.keys():
            self.stream_client = StreamClient(self.client,
                                              account_id=client_parameters['stream_client_parameters']['account_id'],
                                              enforce_enums=False)

            self.level_one_stream_fields = client_parameters['stream_client_parameters']['level_one_stream_fields']

        self.symbol_info = {}

        self.scan_parameters = scan_parameters
        self.scan = not scan_parameters is None
        self.all_tickers = self.get_all_tickers()

        self.notification_parameters = notification_parameters
        self.notify = not notification_parameters is None
        self.alert_sound = alert_sound

        self.watchlist = watchlist
        self.max_watchlist_size = max_watchlist_size
        self.watchlist_space = (max_watchlist_size - len(self.watchlist))
        self.allow_watchlist_reset = True

        if len(watchlist) > 0:
            quotes = json.loads(self.client.get_quotes(watchlist).text)
            for item in list(quotes.values()):
                self.symbol_info[item['symbol']] = item

        self.candle_data = {}
        self.get_candles(watchlist)
        self.compute_indicators(watchlist)

    def get_market_period(self):

        now = datetime.datetime.now()

        #         self.allow_watchlist_reset = self.all_watchlist_reset and (now > (self.scan_start + timedelta(hours=1)))

        if now < self.market_open:
            return 'pre'

        elif now > self.market_close:
            return 'post'

        else:
            return 'regular'

    def scan_tickers(self, data, add_to_symbols=False, max_return_size=None, asset_type=None, exchange=None,
                     min_price=None, max_price=None, daily_volume=None, net_change=None, net_pct_change=None,
                     spread=None, max_bid_size=None, max_ask_size=None, status=None, relative_to_high_price=None):

        tickers = []

        for item in data:

            try:

                if len(tickers) == max_return_size:
                    break

                result = True

                if asset_type:
                    result = (item['assetType'] in asset_type)
                if result and exchange:
                    result = (item['exchange'] in exchange)

                if result and min_price:
                    result = (item['lastPrice'] >= min_price)
                if result and max_price:
                    result = (item['lastPrice'] <= max_price)
                if result and daily_volume:
                    result = (item['totalVolume'] >= daily_volume)
                if result and status:
                    if item['securityStatus'] == 'Halted' and (item['lastPrice'] >= (item['highPrice'] - 1)):
                        tickers.append(item['symbol'])
                        self.symbol_info[item['symbol']] = item
                        continue
                if result and net_change:
                    result = (item['netChange'] >= net_change)
                if result and net_pct_change:
                    result = (item['netPercentChangeInDouble'] >= net_pct_change)
                if result and spread:
                    result = (item['askPrice'] - item['bidPrice'] <= spread)
                if result and max_bid_size:
                    result = (item['bidSize'] <= max_bid_size)
                if result and max_ask_size:
                    result = (item['askSize'] <= max_ask_size)

                if result and relative_to_high_price and (self.market_period == 'regular'):
                    result = (item['lastPrice'] >= (item['highPrice'] - relative_to_high_price))

                if result:
                    tickers.append(item['symbol'])
                    if add_to_symbols:
                        self.symbol_info[item['symbol']] = item

            except Exception as e:
                print('trouble scanning tickers')
                print(e)
                print(item)

        return tickers

    @staticmethod
    def get_batches(data, max_batch_size):

        batches = []
        batch_no = 0
        while (batch_no * max_batch_size < len(data)):
            batches.append(data[batch_no * max_batch_size: (batch_no + 1) * max_batch_size])
            batch_no += 1

        return batches

    def filter_by_fundamentals(self, tickers):

        filtered_tickers = []

        for batch in self.get_batches(tickers, 500):
            fundamentals = json.loads(
                self.client.search_instruments(batch, projection=Client.Instrument.Projection.FUNDAMENTAL).text)
            filtered_tickers.extend(
                self.scan_tickers(list(fundamentals.values()), **self.scan_parameters['fundamental']))

        return filtered_tickers

    def get_all_tickers(self):

        if self.scan_parameters:

            all_instruments = json.loads(
                self.client.search_instruments('[A-Z]*', projection=Client.Instrument.Projection.SYMBOL_REGEX).text)

            tickers = self.scan_tickers(list(all_instruments.values()), **self.scan_parameters['instrument'])

            filtered_tickers = self.filter_by_fundamentals(tickers)

            return filtered_tickers

        else:
            return []

    def update_watchlist(self):

        try:

            check_tickers = [ticker for ticker in self.all_tickers if ticker not in self.watchlist]
            max_new_tickers = min(self.watchlist_space, 10)

            new_tickers = []

            if max_new_tickers > 0:

                for batch in self.get_batches(check_tickers, 500):

                    quotes = json.loads(self.client.get_quotes(batch).text)

                    new_tickers.extend(self.scan_tickers(list(quotes.values()), add_to_symbols=True,
                                                         max_return_size=(max_new_tickers - len(new_tickers)),
                                                         **(self.scan_parameters['quote'])))

                    if (len(new_tickers) >= max_new_tickers):
                        break

                self.add_to_watchlist(new_tickers)

            return new_tickers

        except Exception as e:
            print('error in watchlist function')
            print(e)
            return []

    def add_to_watchlist(self, tickers):

        self.watchlist.extend(tickers)
        self.watchlist_space = (self.max_watchlist_size - len(self.watchlist))

    def remove_from_watchlist(self, tickers=[], clear=False):

        if clear:
            self.watchlist = []
            self.watchlist_space = self.max_watchlist_size

        else:

            for ticker in tickers:
                self.watchlist.remove(ticker)

            self.watchlist_space = (self.max_watchlist_size - len(self.watchlist))

    def get_candles(self, tickers):

        for ticker in tickers:

            try:

                if ticker not in self.candle_data.keys():
                    candles = pd.json_normalize(json.loads(self.client.get_price_history_every_minute(ticker).text),
                                                record_path='candles')
                    self.candle_data[ticker] = candles[candles.datetime >= self.day_start_timestamp].set_index(
                        'datetime', drop=True)[:-1]

            except Exception as e:

                self.remove_from_watchlist([ticker])
                tickers.remove(ticker)
                self.all_tickers.remove(ticker)
                print(f'candle trouble with {ticker}')
                print(e)

    def compute_indicators(self, tickers):

        for ticker in tickers:

            try:

                if ticker in self.candle_data.keys():
                    data = self.candle_data[ticker]

                    high_today = data.high.max()
                    high_regular = data[data.index >= self.market_open_timestamp].high.max()
                    last_1_min_high = data.high.iat[-1]
                    #                     if (data.index[-1] - 240000) % 300000 == 0:
                    #                         last_5_min_high = data[data.index >= data.index[-1] - 240000].high.max()
                    #                     elif 'last_5_min_high' not in self.symbol_info[ticker].keys():
                    #                         for index in data.index[-9:]:
                    #                             if index % 300000 == 0:
                    #                                 last_5_min_high = data[index <= data.index <= index+240000].high.max()
                    rolling_low = data[data.index >= (data.index[-1] - 1500000)].low.min()
                    daily_volume = data[data.index >= self.market_reset_timestamp].volume.sum()
                    total_volume = data.volume.sum()
                    vwap = (data.close * data.volume / total_volume).sum()
                    minute_range = (data.high - data.low)[-5:].mean()

                    self.symbol_info[ticker]['52WkHigh'] = max(self.symbol_info[ticker]['52WkHigh'], high_today)
                    self.symbol_info[ticker]['high_today'] = high_today
                    self.symbol_info[ticker]['high_regular'] = high_regular
                    self.symbol_info[ticker]['last_1_min_high'] = last_1_min_high
                    #                     self.symbol_info[ticker]['last_5_min_high'] = last_5_min_high
                    self.symbol_info[ticker]['rolling_low'] = rolling_low
                    self.symbol_info[ticker]['daily_volume'] = daily_volume
                    self.symbol_info[ticker]['total_volume'] = total_volume
                    self.symbol_info[ticker]['VWAP'] = vwap
                    self.symbol_info[ticker]['minute_range'] = minute_range

                    self.symbol_info[ticker]['Momentum'] = 0.29

                    self.symbol_info[ticker]['Notify'] = ((data.volume.iloc[-5:].median() > 5000) or (
                                data.volume.iat[-1] > 10000)) and (minute_range >= 0.05)
                    self.symbol_info[ticker]['Notify High of Day'] = self.notify
                    self.symbol_info[ticker]['Notify Momentum'] = self.notify and (last_1_min_high > high_today - 2)
                    self.symbol_info[ticker]['Notify Halt'] = self.notify and (last_1_min_high > high_today - 2)
                    self.symbol_info[ticker]['Notify 5 Minute New High'] = self.notify

            except Exception as e:
                print(f'trouble computing indicators for {ticker}')
                print(e)
                self.symbol_info[ticker]['Notify'] = False

    def update_scanner(self):

        new_tickers = self.update_watchlist()
        self.get_candles(new_tickers)
        self.compute_indicators(new_tickers)

        if len(new_tickers) > 0:
            self.alert(f'new tickers added: {", ".join(new_tickers)} at {datetime.datetime.now().strftime("%H:%M:%S")}')

        return new_tickers

    @staticmethod
    def send_notification(email_sender=None, email_receiver=None, email_password=None, subject=None, message=None):

        try:

            em = EmailMessage()
            em['From'] = email_sender
            em['To'] = email_receiver
            em['Subject'] = subject
            em.set_content(message)

            context = ssl.create_default_context()

            with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=context) as smtp:
                smtp.login(email_sender, email_password)
                smtp.sendmail(email_sender, email_receiver, em.as_string())

        except Exception as e:
            print('notification error')
            print(e)

    def alert(self, notification, ticker=None, alert_name=''):

        if not ticker:

            print(notification)

        elif self.symbol_info[ticker]['Notify']:

            print(notification)

            if self.symbol_info[ticker][f'Notify {alert_name}']:

                self.send_notification(**self.notification_parameters, subject=f'Stock {alert_name} Update',
                                       message=notification)
                self.symbol_info[ticker][f'Notify {alert_name}'] = False

                if self.alert_sound:
                    winsound.Beep(*self.alert_sound)

    def high_of_day_scan(self, message):

        quote_time = datetime.datetime.fromtimestamp(message['timestamp'] / 1000).strftime('%H:%M:%S')

        for quote in message['content']:

            try:

                if 'LAST_PRICE' in quote.keys():

                    ticker = quote['key']
                    last_price = quote['LAST_PRICE']

                    if last_price > (self.symbol_info[ticker]['52WkHigh'] + 0.05):

                        self.symbol_info[ticker]['52WkHigh'] = last_price
                        self.symbol_info[ticker]['high_today'] = last_price

                        if self.market_period == 'regular':
                            self.symbol_info[ticker]['high_regular'] = last_price

                        notification = f'{ticker} high of year: ${last_price} at {datetime.datetime.now().strftime("%H:%M:%S")}. Shortable? {self.symbol_info[ticker]["shortable"]}'
                        self.alert(notification, ticker, alert_name='High of Day')

                    elif last_price >= (self.symbol_info[ticker]['high_today'] + 0.05):

                        self.symbol_info[ticker]['high_today'] = last_price

                        if self.market_period == 'regular':
                            self.symbol_info[ticker]['high_regular'] = last_price

                        notification = f'{ticker} high of day: ${last_price} at {quote_time}. Shortable? {self.symbol_info[ticker]["shortable"]}'
                        self.alert(notification, ticker, alert_name='High of Day')

                    elif last_price >= (self.symbol_info[ticker]['high_regular'] + 0.05):

                        self.symbol_info[ticker]['high_regular'] = last_price

                        notification = f'{ticker} high of regular day: ${last_price} at {quote_time}. Shortable? {self.symbol_info[ticker]["shortable"]}'
                        self.alert(notification, ticker, alert_name='High of Day')

            except Exception as e:
                print('high of day scan')
                print(e)

    def halt_scan(self, message):

        quote_time = datetime.datetime.fromtimestamp(message['timestamp'] / 1000).strftime('%H:%M:%S')

        for quote in message['content']:

            try:

                if ('SECURITY_STATUS' in quote.keys()) and (quote['SECURITY_STATUS'] == 'Halted'):
                    ticker = quote['key']

                    notification = f'{ticker} halted at {quote_time}. Shortable? {self.symbol_info[ticker]["shortable"]}'
                    self.alert(notification, ticker, alert_name='Halt')

            except Exception as e:
                print('halt scan')
                print(e)

    def momentum_scan(self, message):

        quote_time = datetime.datetime.fromtimestamp(message['timestamp'] / 1000).strftime('%H:%M:%S')

        for quote in message['content']:

            try:

                if 'LAST_PRICE' in quote.keys():

                    ticker = quote['key']

                    last_price = quote['LAST_PRICE']
                    last_high = self.symbol_info[ticker]['last_1_min_high']

                    if last_price > (last_high + self.symbol_info[ticker]['Momentum']) and last_price > \
                            self.symbol_info[ticker]['VWAP']:
                        self.symbol_info[ticker]['Momentum'] = last_price - last_high

                        notification = f'{ticker} (${last_price}) up ${round(last_price - last_high, 2)} in last minute at {quote_time}. Shortable? {self.symbol_info[ticker]["shortable"]}'
                        self.alert(notification, ticker, alert_name='Momentum')

            except Exception as e:
                print('momentum scan')
                print(e)

    def five_minute_new_high_scan(self, message):

        quote_time = datetime.datetime.fromtimestamp(message['timestamp'] / 1000).strftime('%H:%M:%S')

        for quote in message['content']:

            try:

                if 'LAST_PRICE' in quote.keys():
                    ticker = quote['key']

                    last_price = quote['LAST_PRICE']
                    last_high = self.symbol_info[ticker]['last_5_min_high']
                    rolling_low = self.symbol_info[ticker]['rolling_low']

            #                         if last_price >= (last_high + self.symbol_info[ticker]['Momentum']) and last_price > self.symbol_info[ticker]['VWAP']:

            #                             self.symbol_info[ticker]['Momentum'] = last_price - last_high

            #                             notification = f'{ticker} up ${round(last_price-last_high, 2)} in last minute at {quote_time}. Shortable? {self.symbol_info[ticker]["shortable"]}'
            #                             self.alert(ticker, notification, alert_name='Momentum')

            except Exception as e:
                print('5 minute scan')
                print(e)

    async def candlestick_handler(self, message):

        self.market_period = self.get_market_period()

        if (datetime.datetime.now().second < 10):

            for item in message['content']:
                ticker = item['key']

                candle = [item['OPEN_PRICE'], item['HIGH_PRICE'], item['LOW_PRICE'], item['CLOSE_PRICE'],
                          item['VOLUME']]

                self.candle_data[ticker].loc[item['CHART_TIME']] = candle

            self.compute_indicators(self.watchlist)

            if self.scan:

                if (self.watchlist_space <= 0) and self.allow_watchlist_reset and (self.market_period == 'regular'):
                    print('watchlist full, resetting watchlist')

                    await self.unsubscribe(self.watchlist)

                    self.remove_from_watchlist(clear=True)
                    self.allow_watchlist_reset = False

                new_tickers = self.update_scanner()

                if len(new_tickers) > 0:

                    try:

                        await self.stream_client.chart_equity_add(new_tickers)
                        await self.stream_client.level_one_equity_subs(self.watchlist,
                                                                       fields=self.level_one_stream_fields)

                    except Exception as e:
                        print('trouble adding to streams')
                        print(e)

    def level_one_handler(self, message):

        self.high_of_day_scan(message)
        self.halt_scan(message)
        self.momentum_scan(message)

    #         self.five_minute_new_high_scan(message)

    async def unsubscribe(self, tickers):

        await self.stream_client.chart_equity_unsubs(tickers)
        await self.stream_client.level_one_equity_unsubs(tickers)

    async def stream(self):

        await self.stream_client.login()
        await self.stream_client.quality_of_service('0')

        self.stream_client.add_chart_equity_handler(self.candlestick_handler)
        self.stream_client.add_level_one_equity_handler(self.level_one_handler)

        while True:

            if self.scan:
                self.update_scanner()

            if len(self.watchlist) > 0:
                await self.stream_client.chart_equity_subs(self.watchlist)
                await self.stream_client.level_one_equity_subs(self.watchlist, fields=self.level_one_stream_fields)

                break

            print(f'no qualifying tickers as of {datetime.datetime.now().strftime("%H:%M:%S")}')
            sleep(30)

        while True:
            await self.stream_client.handle_message()


API_KEY = ''
REDIRECT_URI = 'http://localhost:3000/auth'
TOKEN_PATH = 'tdameritrade_token'
ACCOUNT_ID = ''

email_sender = ''
email_receiver = ''
email_password = ''

client_parameters = {'client_parameters': {'api_key': API_KEY,
                                           'redirect_uri': REDIRECT_URI,
                                           'token_path': TOKEN_PATH},
                     'stream_client_parameters': {'account_id': ACCOUNT_ID,
                                                  'level_one_stream_fields': [1, 2, 3, 4, 5, 8, 12, 13, 15, 28, 29, 30, 31, 48, 49]}}

scan_parameters = {'instrument': {'asset_type': ['EQUITY'],
                                  'exchange': ['NYSE', 'NASDAQ']},
                   'quote': {'min_price': 2,
                             'max_price': 40,
                             'daily_volume': 200000,
                             'net_change': 0.50,
                             'net_pct_change': 5,
                             'spread': 0.5,
                             'max_bid_size': 10000,
                             'max_ask_size': 10000,
                             'status': True,
                             'relative_to_high_price': 1},
                   'fundamental': {}}

notification_parameters = {'email_sender': email_sender,
                           'email_receiver': email_receiver,
                           'email_password': email_password}

watchlist = []
alert_sound = (600, 250)

scanner = TDAStockScanner(client_parameters, scan_parameters=scan_parameters, notification_parameters=notification_parameters)

await scanner.stream()
