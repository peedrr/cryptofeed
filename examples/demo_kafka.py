'''
Copyright (C) 2018-2022 Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
from cryptofeed import FeedHandler
from cryptofeed.backends.kafka import BookKafka, CandlesKafka, FundingKafka, OpenInterestKafka, TradeKafka, TickerKafka, LiquidationsKafka
from cryptofeed.defines import L2_BOOK, CANDLES, TRADES, TICKER, FUNDING, OPEN_INTEREST, LIQUIDATIONS
from cryptofeed.exchanges import Coinbase
from cryptofeed.exchanges.okx import OKX


"""
This Kafka backend uses the following CustomizeMixin features:

- topic and key accept template strings.

The template strings can contain the name of any str element found in types.pyx and/or the keyword `channel`. 
These keywords will be replaced at runtime with the data from the exchange. 
The templates also preserve case. E.g. `SYMBOL` = `BTC-USD`, `symbol` = `btc-usd`  
See docs/customize_mixin.md for more details

---
The AIOKafkaProducer accepts configuration options passed as kwargs to the Kafka callback(s)
either as individual kwargs, an unpacked dictionary `**config_dict`, or both, as in the example below.
The full list of configuration parameters can be found at
https://aiokafka.readthedocs.io/en/stable/api.html#aiokafka.AIOKafkaProducer  

You can run a Kafka consumer in the console with the following command
(assuminng the defaults for the consumer group and bootstrap server)

$ kafka-console-consumer --bootstrap-server 127.0.0.1:9092 --topic trades-COINBASE-BTC-USD
"""


def main():
    common_kafka_config = {
        'bootstrap_servers': '127.0.0.1:9092',
        'acks': 1,
        'request_timeout_ms': 10000,
        'connections_max_idle_ms': 20000,
    }
    f = FeedHandler({'log':{'filename': 'feedhandler.log', 'level': 'INFO'}})
    coinbase_cbs = {
        L2_BOOK: BookKafka(topic='EXCHANGE-Channel-Symbol', key='cryptofeed-symbol', client_id='Coinbase Book', **common_kafka_config),
        TRADES: TradeKafka(topic='Exchange-CHANNEL-SYMBOL', key='cryptofeed-symbol-side', client_id='Coinbase Trades', **common_kafka_config),
        TICKER: TickerKafka(client_id='Coinbase Ticker', **common_kafka_config)
    }

    okx_cbs = {
        L2_BOOK: BookKafka(topic='EXCHANGE-Channel-symbol', client_id='OKX Book', **common_kafka_config),
        TICKER: TickerKafka(topic='exchange-channel-SYMBOL', client_id='OKX Ticker', **common_kafka_config), 
        LIQUIDATIONS: LiquidationsKafka(topic='Exchange-Channel-Side-Status', **common_kafka_config), 
        FUNDING: FundingKafka(topic='exchange-channel-SYMBOL', **common_kafka_config), 
        OPEN_INTEREST: OpenInterestKafka(topic='EXCHANGE-channel-symbol', **common_kafka_config), 
        TRADES: TradeKafka(topic='exchange-channel', **common_kafka_config),
        CANDLES: CandlesKafka(key='SYMBOL', **common_kafka_config),
    }
    
    
    

    f.add_feed(Coinbase(max_depth=10, channels=[TRADES, L2_BOOK, TICKER], symbols=['BTC-USD', 'ETH-USD', 'SOL-USD'], callbacks=coinbase_cbs))
    f.add_feed(OKX(checksum_validation=True, symbols=['BTC-USDT-PERP'], channels=[CANDLES, TRADES, TICKER, FUNDING, OPEN_INTEREST, LIQUIDATIONS, L2_BOOK], callbacks=okx_cbs))

    f.run()


if __name__ == '__main__':
    main()
