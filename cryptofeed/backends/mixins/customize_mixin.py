from typing import Any, List, Optional, Union
import logging


LOG = logging.getLogger('feedhandler')


class CustomizeMixin:

    STANDARD_KEYWORDS = ['exchange', 'channel']
    DYNAMIC_KEYWORDS = "symbol side type status interval client_order_id account currency order_id liquidity".split()
    VALID_ATTR = {
        'trades': {'strings': ['exchange', 'symbol', 'side', 'id', 'type'], 'others': ['price', 'amount', 'timestamp', 'raw']},
        'funding': {'strings': ['exchange', 'symbol'], 'others': ['mark_price', 'rate', 'next_funding_time', 'predicted_rate', 'timestamp', 'raw']},
        'book': {'strings': ['exchange', 'symbol'], 'others': ['book', 'delta', 'sequence_number', 'checksum', 'timestamp', 'raw']},
        'ticker': {'strings': ['exchange', 'symbol'], 'others': ['bid', 'ask', 'timestamp', 'raw']},
        'open_interest': {'strings': ['exchange', 'symbol'], 'others': ['open_interest', 'timestamp', 'raw']},
        'liquidations': {'strings': ['exchange', 'symbol', 'side', 'id', 'status', ], 'others': ['quantity', 'price', 'timestamp', 'raw']},
        'candles': {'strings': ['exchange', 'symbol', 'interval'], 'others': ['start', 'stop', 'trades', 'open', 'close', 'high', 'low', 'volume', 'closed', 'timestamp', 'raw']},
        'order_info': {'strings': ['exchange', 'symbol', 'id', 'client_order_id', 'side', 'status', 'type', 'account'], 'others': ['price', 'amount', 'remaining', 'timestamp', 'raw']},
        'transactions': {'strings': ['exchange', 'currency', 'type', 'status'], 'others': ['amount', 'timestamp', 'raw']},
        'balances': {'strings': ['exchange', 'currency'], 'others': ['balance', 'reserved', 'raw']},
        'fills': {'strings': ['exchange', 'symbol', 'side', 'id', 'order_id', 'liquidity', 'type', 'account'], 'others': ['price', 'amount', 'fee', 'timestamp', 'raw']},
    }

    def valid_template(self, str_type: str, user_template: str) -> str:
        """
        Verifies that the user template contains valid keywords
        i.e. str type attributes which exist in the expected data type.
        
        str_type: the purpose of the required string e.g. 'topic' (Kafka), 'key' (Redis), 'subject' (NATS)
        user_template: the user-provided template string
        """
        keys_in_template = [word.lower() for word in self._case_generator(self.DYNAMIC_KEYWORDS) if (word in user_template)]
        for key in keys_in_template:
            if key not in self.VALID_ATTR[self.channel_name]['strings']:
                LOG.error(TypeError(f"{self.__class__.__name__} : {self.channel_name.capitalize()} {str_type} invalid: {self.channel_name.capitalize()} data type has no '{key}' data. Check valid (str only) options in types.pyx"))
                raise SystemExit
        return user_template

    def valid_targets(self, data_targets:dict[str, Any]) -> dict[str, Any]:
        """
        Verifies that the keys of the user-supplied `data_targets` dictionary are valid for the data type of the channel.
        """
        for k in data_targets:
            if k not in [*self.VALID_ATTR[self.channel_name]['strings'], *self.VALID_ATTR[self.channel_name]['others']]:
                LOG.error(TypeError(f"{self.__class__.__name__} : 'data_targets' check failed: '{k}' is not a valid attribute for '{self.channel_name}' data. Please check valid options in types.pyx"))
                raise SystemExit
        return data_targets

    def get_formatted_string(self, str_type: str, user_template: Optional[str], data: dict):
        """
        Takes a user template and returns a (customized) string

        str_type: the purpose of the required string e.g. 'topic' (Kafka), 'key' (Redis), 'subject' (NATS)
        user_template: the user-provided template string
        data: the data dict containing the string elements to swap in
        """
        # First, attempt to retrieve an existing key from the store
        retrieved_string = self.custom_strings.get(str_type) or self.custom_strings.get(f"{str_type}-{'-'.join([data.get(key) for key in self.custom_string_keys.get(str_type, [])])}")
        if retrieved_string:
            return retrieved_string

        if not user_template:
            self.custom_strings[str_type] = self.channel_name
            return self.channel_name

        else:  # create new string
            standard_string = self._customize_string(user_template, data, dynamic=False)
            is_dynamic = self._check_dynamic(standard_string, str_type)

            if is_dynamic:
                dynamic_string, key_list = self._customize_string(standard_string, data, dynamic=True)
                try:
                    self.custom_strings[f"{str_type}-{'-'.join([data[key] for key in key_list])}"] = dynamic_string
                except KeyError as e:
                    LOG.error(f"{self.__class__.__name__} : Your {str_type} template '{user_template}' includes the {e} keyword. {self.channel_name.capitalize()} data has no {e} value. Remove it from your custom {str_type}")
                    self.stop()
                else:
                    return dynamic_string
            else:
                self.custom_strings[str_type] = standard_string
                return standard_string

    def customize_data_package(self, data: dict[str, Any]) -> dict[str, Any]:
        """
        This method formats the exchange data based on the items included in
        the user-supplied `data_targets` dict. The resulting dictionary maintains the order
        of data from the user's dictionary, and applies new keys/labels to each data point,
        if provided
        """
        return {v: data[k] for k, v in self.data_targets.items()}

    def _customize_string(self, user_template: str, data: dict, dynamic: bool):
        swap_list = self._find_swaps(user_template, dynamic=dynamic)
        custom_string = self._swap_items(user_template, swap_list, data)
        if dynamic:
            return custom_string, [i.lower() for i in swap_list]
        return custom_string

    def _case_generator(self, words: Union[str, List]):
        cases = [str.upper, str.lower, str.title]

        if isinstance(words, str):
            words = list(words)

        for word in words:
            for case in cases:
                yield case(word)

    def _check_case(self, item: str):
        if item.islower():
            return str.lower

        elif item.istitle():
            return str.title

        elif item.isupper():
            return str.upper

    def _check_dynamic(self, user_str: str, str_type: str) -> bool:
        self.custom_string_keys[str_type] = [item.lower() for item in self._case_generator(self.DYNAMIC_KEYWORDS) if (item in user_str)]
        return True if self.custom_string_keys[str_type] else False

    def _find_swaps(self, user_template: str, dynamic: bool = False) -> List[str]:
        check_list = self.DYNAMIC_KEYWORDS if dynamic else self.STANDARD_KEYWORDS
        swap_list = [word for word in self._case_generator(check_list) if (word in user_template)]
        return swap_list

    def _swap_items(self, user_str: str, swap_list: list, data: dict) -> str:
        altered_string = user_str
        for item in swap_list:
            format_case = self._check_case(item)
            replacement = str()
            replacement = format_case(data.get(item.lower(), self.channel_name))
            altered_string = altered_string.replace(item, replacement)
        return altered_string
