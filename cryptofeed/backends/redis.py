'''
Copyright (C) 2017-2022 Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
from collections import defaultdict
from typing import Dict, Optional

import aioredis
from yapic import json

from cryptofeed.backends.backend import BackendBookCallback, BackendCallback, BackendQueue
from cryptofeed.backends.mixins.customize_mixin import CustomizeMixin


class RedisCallback(CustomizeMixin, BackendQueue):
    def __init__(self, host='127.0.0.1', port=6379, socket=None, key: Optional[str] = None, data_targets: Optional[Dict[str, str]] = None, none_to='None', numeric_type=float, **kwargs):
        """
        This callback uses the following `CustomizeMixin` features:
            - `key` accepts template strings.
            - `data_targets` is a dictionary for selecting, relabeling and/or reordering available data types
                    
        See docs/customize_mixin.md for more details
        
        ---
        setting key lets you override the prefix on the
        key used in redis. The defaults are related to the data
        being stored, i.e. trade, funding, etc
        """
        prefix = 'redis://'
        if socket:
            prefix = 'unix://'
            port = None

        self.redis = f"{prefix}{host}" + f":{port}" if port else ""
        self.numeric_type = numeric_type
        self.none_to = none_to
        self.running = True
        # ---------- Instance variables for CustomizeMixin ----------
        self.key_template = self.valid_template('key', key) if key else None
        self.data_targets = self.valid_targets(data_targets) if data_targets else None
        self.custom_strings: Dict[str, str] = dict()  # Dict store for fast retrieval of customised and/or dynamic strings
        self.custom_string_keys: Dict[str, list] = dict()  # Lists of keys to use when searching custom_strings dict
        # -----------------------------------------------------------



class RedisZSetCallback(RedisCallback):
    def __init__(self, host='127.0.0.1', port=6379, socket=None, key=None, numeric_type=float, score_key='timestamp', **kwargs):
        """
        score_key: str
            the value at this key will be used to store the data in the ZSet in redis. The
            default is timestamp. If you wish to look up the data by a different value,
            use this to change it. It must be a numeric value.
        """
        self.score_key = score_key
        super().__init__(host=host, port=port, socket=socket, key=key, numeric_type=numeric_type, **kwargs)

    async def writer(self):
        conn = aioredis.from_url(self.redis)

        while self.running:
            async with self.read_queue() as updates:
                async with conn.pipeline(transaction=False) as pipe:
                    for update in updates:
                        key = self.get_formatted_string('key', self.key_template, update) if self.key_template else self.channel_name
                        # Default score_key is timestamp which could potentially be removed update by subsequent if statement
                        score_key = update[self.score_key]
                        # If provided, format data package as per user specification
                        if self.data_targets:
                            update = self.customize_data_package(update) 
                        pipe = pipe.zadd(f"{key}", {json.dumps(update): score_key}, nx=True)
                    await pipe.execute()

        await conn.close()
        await conn.connection_pool.disconnect()


class RedisStreamCallback(RedisCallback):
    async def writer(self):
        conn = aioredis.from_url(self.redis)

        while self.running:
            async with self.read_queue() as updates:
                async with conn.pipeline(transaction=False) as pipe:
                    for update in updates:
                        key = self.get_formatted_string('key', self.key_template, update) if self.key_template else self.channel_name
                        if 'delta' in update:
                            update['delta'] = json.dumps(update['delta'])
                        elif 'book' in update:
                            update['book'] = json.dumps(update['book'])
                        elif 'closed' in update:
                            update['closed'] = str(update['closed'])

                        # If provided, format data package as per user specification
                        if self.data_targets:
                            update = self.customize_data_package(update)
                        pipe = pipe.xadd(f"{key}", update)
                    await pipe.execute()

        await conn.close()
        await conn.connection_pool.disconnect()


class TradeRedis(RedisZSetCallback, BackendCallback):
    channel_name = 'trades'


class TradeStream(RedisStreamCallback, BackendCallback):
    channel_name = 'trades'


class FundingRedis(RedisZSetCallback, BackendCallback):
    channel_name = 'funding'


class FundingStream(RedisStreamCallback, BackendCallback):
    channel_name = 'funding'


class BookRedis(RedisZSetCallback, BackendBookCallback):
    channel_name = 'book'

    def __init__(self, *args, snapshots_only=False, snapshot_interval=1000, score_key='receipt_timestamp', **kwargs):
        self.snapshots_only = snapshots_only
        self.snapshot_interval = snapshot_interval
        self.snapshot_count = defaultdict(int)
        super().__init__(*args, score_key=score_key, **kwargs)


class BookStream(RedisStreamCallback, BackendBookCallback):
    channel_name = 'book'

    def __init__(self, *args, snapshots_only=False, snapshot_interval=1000, **kwargs):
        self.snapshots_only = snapshots_only
        self.snapshot_interval = snapshot_interval
        self.snapshot_count = defaultdict(int)
        super().__init__(*args, **kwargs)


class TickerRedis(RedisZSetCallback, BackendCallback):
    channel_name = 'ticker'


class TickerStream(RedisStreamCallback, BackendCallback):
    channel_name = 'ticker'


class OpenInterestRedis(RedisZSetCallback, BackendCallback):
    channel_name = 'open_interest'


class OpenInterestStream(RedisStreamCallback, BackendCallback):
    channel_name = 'open_interest'


class LiquidationsRedis(RedisZSetCallback, BackendCallback):
    channel_name = 'liquidations'


class LiquidationsStream(RedisStreamCallback, BackendCallback):
    channel_name = 'liquidations'


class CandlesRedis(RedisZSetCallback, BackendCallback):
    channel_name = 'candles'


class CandlesStream(RedisStreamCallback, BackendCallback):
    channel_name = 'candles'


class OrderInfoRedis(RedisZSetCallback, BackendCallback):
    channel_name = 'order_info'


class OrderInfoStream(RedisStreamCallback, BackendCallback):
    channel_name = 'order_info'


class TransactionsRedis(RedisZSetCallback, BackendCallback):
    channel_name = 'transactions'


class TransactionsStream(RedisStreamCallback, BackendCallback):
    channel_name = 'transactions'


class BalancesRedis(RedisZSetCallback, BackendCallback):
    channel_name = 'balances'


class BalancesStream(RedisStreamCallback, BackendCallback):
    channel_name = 'balances'


class FillsRedis(RedisZSetCallback, BackendCallback):
    channel_name = 'fills'


class FillsStream(RedisStreamCallback, BackendCallback):
    channel_name = 'fills'
