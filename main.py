#  Copyright (c) 2022 James Hartman. All Rights Reserved
#  main.py
#  OrderBookFeed
#
#  Written by James Hartman <JamesLouisHartman@gmail.com.au>
#  Last modified 24/4/22, 8:48 pm
#
#  Written by James Hartman <JamesLouisHartman@gmail.com.au>
#  Last modified 24/4/22, 6:27 pm
#
#  Written by James Hartman <JamesLouisHartman@gmail.com.au>
#  Last modified 24/4/22, 12:48 pm

#  main.py
#  OrderBookFeed


# note:  only print snapshot of N most important levels for SYMBOL if top N of SYMBOL changes

# done: 1 input stream parsing
# done: 2 pass to def for messages
# done: 3 handle messages for add, update, delete and execute
# done: 4 keep track of order book
# done: 5 trigger print when change occurs in top n of buy or sell
# TODO: 6 do potential (possible) error handling
# TODO: 7 cleanup
# TODO: 8 optimise, brute force first
# TODO: 9 optional: build for continuous input, i.e. unending, true stream of data
# TODO: 10 optional: stream corruption checking

BUY_STR = 'B'
SELL_STR = 'S'

import heapq
from operator import itemgetter


class OrderBook:

    def __init__(self, n):
        self.symbols = {}
        self.levels = n
        self.snapshot = {}
        # {"symbol": {"buys": {"order_id": {"size": "size", "price": "price"}, ...}, ...}}

    # region Actions
    def add(self, symbol, side, order_id, price, size):
        # create a new order, error if exists
        if symbol in self.symbols:
            if side in self.symbols[symbol]:
                if order_id in self.symbols[symbol][side]:
                    raise KeyError
                else:
                    self.symbols[symbol][side][order_id] = {"price": price, "size": size, "order_id": order_id}
            else:
                self.symbols[symbol][side] = {order_id: {"price": price, "size": size, "order_id": order_id}}
        else:
            self.symbols[symbol] = {side: {order_id: {"price": price, "size": size, "order_id": order_id}}}

    def update(self, symbol, side, order_id, price, size):
        # update an existing order, error if not exists
        self.symbols[symbol][side][order_id] = {"price": price, "size": size}

    def delete(self, symbol, side, order_id, size = None):
        # delete all or execute an amount from an order
        if size is None:
            # delete all
            del self.symbols[symbol][side][order_id]
        else:
            # execute some or all
            # needed as for some reason -= would modify the snapshot too
            _old_price = self.symbols[symbol][side][order_id]["price"]
            _new_size = self.symbols[symbol][side][order_id]["size"] - size
            self.update(symbol, side, order_id, _old_price, _new_size)
            if self.symbols[symbol][side][order_id]["size"] <= 0:
                del self.symbols[symbol][side][order_id]

    # endregion

    def take_action(self, action, sequence_num):
        if action['type'] == 'A':
            # ADD
            self.add(action["symbol"], action["side"], action["order"], action["price"], action["size"])
        elif action['type'] == 'U':
            # UPDATE
            self.update(action["symbol"], action["side"], action["order"], action["price"], action["size"])
        elif action['type'] == 'D':
            # DELETE
            self.delete(action["symbol"], action["side"], action["order"])
        else:
            # EXECUTE
            self.delete(action["symbol"], action["side"], action["order"], action["size"])
        self.check_snapshot(action["symbol"], action["side"], sequence_num)

    def check_snapshot(self, symbol, side, sequence_num):
        # for buy or sell in symbol (based on side): get n highest/lowest orders (based on price)
        _n = self.levels
        if len(self.symbols[symbol][side]) > 0:
            if side == BUY_STR:
                # highest_buys
                update = heapq.nlargest(_n, list(self.symbols[symbol][side].values()), key = itemgetter('price'))
            else:
                # lowest sells
                update = heapq.nsmallest(_n, list(self.symbols[symbol][side].values()), key = itemgetter('price'))
        else:
            update = []  # save time if blank, which could actually be extended to 1, since len 1 is always "ordered

        if symbol in self.snapshot and side in self.snapshot[symbol]:
            if update != self.snapshot[symbol][side]:
                self.update_snapshot(symbol, side, update)
                self.print_snapshot(symbol, sequence_num)
        else:
            # new symbol, therefore it must be new
            self.update_snapshot(symbol, side, update)
            self.print_snapshot(symbol, sequence_num)

    def update_snapshot(self, symbol, side, update):
        if symbol in self.snapshot:
            self.snapshot[symbol][side] = update
        else:
            self.snapshot[symbol] = {side: update}

    def print_snapshot(self, symbol, sequence_num):
        # noinspection Duplicates
        if BUY_STR in self.snapshot[symbol]:
            _buy_str = '['
            if len(self.snapshot[symbol][BUY_STR]) > 0:
                for order in self.snapshot[symbol][BUY_STR]:
                    _buy_str += f'({order["price"]}, {order["size"]}), '
                _buy_str = _buy_str[:-2]
            _buy_str += ']'
        else:
            _buy_str = '[]'

        # noinspection Duplicates
        if SELL_STR in self.snapshot[symbol]:
            _sell_str = '['
            if len(self.snapshot[symbol][SELL_STR]) > 0:
                for order in self.snapshot[symbol][SELL_STR]:
                    _sell_str += f'({order["price"]}, {order["size"]}), '
                _sell_str = _sell_str[:-2]
            _sell_str += ']'
        else:
            _sell_str = '[]'
        print(f'{sequence_num}, {symbol}, {_buy_str}, {_sell_str}')

    def print_out(self):
        print(self.symbols)


def read_n_bytes(mutable_bytes, num_bytes):
    """

    Args:
        mutable_bytes (bytearray):
        num_bytes (int):

    Returns:
        bytearray, bytearray
    """
    read_bytes: bytearray = mutable_bytes[:num_bytes]
    remaining_bytes: bytearray = mutable_bytes[num_bytes:]
    return remaining_bytes, read_bytes


# could use a class, or just global it lol


def parse_msg(mutable_bytes):
    """

    Args:
        mutable_bytes (bytearray):

    Returns:
        dict
    """
    # shared by all: type, symbol, orderID, side, (padding)
    if len(mutable_bytes) < 16:  # bare minimum message size, according to documentation
        print(f'VALUE ERROR: length {len(mutable_bytes)}')
        raise ValueError  # TODO: refer to todo 10
    mutable_bytes, _type_byte = read_n_bytes(mutable_bytes, 1)
    mutable_bytes, _symbol_bytes = read_n_bytes(mutable_bytes, 3)
    mutable_bytes, _order_bytes = read_n_bytes(mutable_bytes, 8)
    mutable_bytes, _side_byte = read_n_bytes(mutable_bytes, 1)
    mutable_bytes, _reserved_bytes = read_n_bytes(mutable_bytes, 3)  # reserved bytes are unused

    _message = {'type' : _type_byte.decode("utf-8"), 'symbol': _symbol_bytes.decode("utf-8"),
                'order': int.from_bytes(_order_bytes, byteorder = 'little', signed = False),
                'side' : _side_byte.decode("utf-8")}
    if _message['type'] in ['A', 'U', 'E']:
        mutable_bytes, _size_bytes = read_n_bytes(mutable_bytes, 8)
        _message['size'] = int.from_bytes(_size_bytes, byteorder = 'little', signed = False)
        if _message['type'] in ['A', 'U']:
            mutable_bytes, _price_bytes = read_n_bytes(mutable_bytes, 4)
            _message['price'] = int.from_bytes(_price_bytes, byteorder = 'little', signed = True)
            # remaining bytes can be discarded

    return _message


if __name__ == '__main__':
    # parse command line
    # TODO: fix this as it corrupts when passing as cat | program

    foo = open("input2.stream", "rb").read()
    mutable_foo = bytearray(foo)

    # todo: make argument
    levels = 5

    order_book = OrderBook(levels)

    # we always start with a header of 4 bytes
    # original_len = len(mutable_foo)
    while len(mutable_foo) > 8:
        # Any smaller value would not be able to fit a header.
        # This minimum length could be larger, depending on the absolute minimum size of a message:
        # Theoretically zero. However, the minimum length for any MEANINGFUL message is 13 (16 with padding)
        # Note: doing some waiting magic, it could be possible to use an unending stream, at variable rates
        # TODO: look into 'import struct' for easier byte reading
        mutable_foo, seq_bytes = read_n_bytes(mutable_foo, 4)
        mutable_foo, size_bytes = read_n_bytes(mutable_foo, 4)

        seq_num = int.from_bytes(seq_bytes, byteorder = 'little', signed = False)
        msg_size = int.from_bytes(size_bytes, byteorder = 'little', signed = False)

        mutable_foo, msg_bytes = read_n_bytes(mutable_foo, msg_size)
        order_book.take_action(parse_msg(msg_bytes), seq_num)
