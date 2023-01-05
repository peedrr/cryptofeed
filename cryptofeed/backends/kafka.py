'''
Copyright (C) 2017-2022 Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
from collections import defaultdict
import asyncio
import logging
from typing import Dict, Optional, ByteString

from aiokafka import AIOKafkaProducer
from aiokafka.errors import RequestTimedOutError, KafkaConnectionError, NodeNotReadyError
from yapic import json

from cryptofeed.backends.backend import BackendBookCallback, BackendCallback, BackendQueue
from cryptofeed.backends.mixins.customize_mixin import CustomizeMixin

LOG = logging.getLogger('feedhandler')


class KafkaCallback(CustomizeMixin, BackendQueue):
    def __init__(self, topic: Optional[str] = None, key: Optional[str] = None, data_targets: Optional[Dict[str, str]] = None, numeric_type=float, none_to=None, **kwargs):
        """
        This callback uses the following `CustomizeMixin` features:
            - `topic` and `key` accept template strings.
            - `data_targets` is a dictionary for selecting, relabeling and/or reordering available data types
                    
        See docs/customize_mixin.md for more details
        
        ---
        You can pass configuration options to AIOKafkaProducer as keyword arguments.
        (either individual kwargs, an unpacked dictionary `**config_dict`, or both)
        A full list of configuration parameters can be found at
        https://aiokafka.readthedocs.io/en/stable/api.html#aiokafka.AIOKafkaProducer  
        
        A 'value_serializer' option allows use of other schemas such as Avro, Protobuf etc. 
        The default serialization is JSON Bytes
        
        Example:
        
            **{'bootstrap_servers': '127.0.0.1:9092',
            'client_id': 'cryptofeed',
            'acks': 1,
            'value_serializer': your_serialization_function}
            
        (Passing the event loop is already handled)
        """
        self.producer_config = kwargs
        self.producer = None

        self.numeric_type = numeric_type
        self.none_to = none_to
        # Do not allow writer to send messages until connection confirmed
        self.running = False
        
        # ---------- Instance variables for CustomizeMixin ----------
        # Store user-supplied key/topic in temp variable before processing
        self.key_template: Optional[str] = self.valid_template(key, 'key') if key else None
        self.topic_template: Optional[str] = self.valid_template(topic, 'topic') if topic else None
        self.custom_strings: Dict[str, str] = dict()  # Dict store for fast retrieval of customised or dynamic keys/topics
        self.custom_string_keys: Dict[str, list] = dict()  # Lists of keys to use when searching string dict
        
        self.data_targets = self.valid_targets(data_targets) if data_targets else None
        # -----------------------------------------------------------
        
    
    def _default_serializer(self, to_bytes: dict | str) -> ByteString:
        if isinstance(to_bytes, dict):
            return json.dumpb(to_bytes)
        elif isinstance(to_bytes, str):
            return to_bytes.encode()
        else:
            raise TypeError(f'{type(to_bytes)} is not a valid Serialization type')

    async def _connect(self):
        if not self.producer:
            loop = asyncio.get_event_loop()
            try:
                config_keys = ', '.join([k for k in self.producer_config.keys()])
                LOG.info(f'{self.__class__.__name__}: Configuring AIOKafka with the following parameters: {config_keys}')
                self.producer = AIOKafkaProducer(**self.producer_config, loop=loop)
            # Quit if invalid config option passed to AIOKafka
            except (TypeError, ValueError) as e:
                LOG.error(f'{self.__class__.__name__}: Invalid AIOKafka configuration: {e.args}{chr(10)}See https://aiokafka.readthedocs.io/en/stable/api.html#aiokafka.AIOKafkaProducer for list of configuration options')
                raise SystemExit
            else:
                while not self.running:
                    try:
                        await self.producer.start()
                    except KafkaConnectionError:
                        LOG.error(f'{self.__class__.__name__}: Unable to bootstrap from host(s)')
                        await asyncio.sleep(10)
                    else:
                        LOG.info(f'{self.__class__.__name__}: "{self.producer.client._client_id}" connected to cluster containing {len(self.producer.client.cluster.brokers())} broker(s)')
                        self.running = True

    def partition(self, data: dict) -> Optional[int]:
        return None

    async def writer(self):
        await self._connect()
        while self.running:
            async with self.read_queue() as updates:
                for index in range(len(updates)):
                    data = updates[index]
                    #First, format topic and key in case required data subsequently removed when applying data_targets
                    topic = self.get_formatted_string('topic', self.topic_template, data)
                    key = self.get_formatted_string('key', self.key_template, data) if self.key_template else self.channel_name

                    # If provided, format data package as per user specification
                    if self.data_targets:
                        data = self.customize_data_package(data)

                    # Check for user-provided serializers, otherwise use default
                    value = data if self.producer_config.get('value_serializer') else self._default_serializer(data)
                    key = key if self.producer_config.get('key_serializer') else self._default_serializer(key)
                    partition = self.partition(data)
                    try:
                        send_future = await self.producer.send(topic, value, key, partition)
                        await send_future
                    except RequestTimedOutError:
                        LOG.error(f'{self.__class__.__name__}: No response received from server within {self.producer._request_timeout_ms} ms. Messages may not have been delivered')
                    except NodeNotReadyError:
                        LOG.error(f'{self.__class__.__name__}: Node not ready')
                    except Exception as e:
                        LOG.info(f'{self.__class__.__name__}: Encountered an error:{chr(10)}{e}')
        LOG.info(f"{self.__class__.__name__}: sending last messages and closing connection '{self.producer.client._client_id}'")
        await self.producer.stop()


class TradeKafka(KafkaCallback, BackendCallback):
    channel_name = 'trades'


class FundingKafka(KafkaCallback, BackendCallback):
    channel_name = 'funding'


class BookKafka(KafkaCallback, BackendBookCallback):
    channel_name = 'book'

    def __init__(self, *args, snapshots_only=False, snapshot_interval=1000, **kwargs):
        self.snapshots_only = snapshots_only
        self.snapshot_interval = snapshot_interval
        self.snapshot_count = defaultdict(int)
        super().__init__(*args, **kwargs)


class TickerKafka(KafkaCallback, BackendCallback):
    channel_name = 'ticker'


class OpenInterestKafka(KafkaCallback, BackendCallback):
    channel_name = 'open_interest'


class LiquidationsKafka(KafkaCallback, BackendCallback):
    channel_name = 'liquidations'


class CandlesKafka(KafkaCallback, BackendCallback):
    channel_name = 'candles'


class OrderInfoKafka(KafkaCallback, BackendCallback):
    channel_name = 'order_info'


class TransactionsKafka(KafkaCallback, BackendCallback):
    channel_name = 'transactions'


class BalancesKafka(KafkaCallback, BackendCallback):
    channel_name = 'balances'


class FillsKafka(KafkaCallback, BackendCallback):
    channel_name = 'fills'
