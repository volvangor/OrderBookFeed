#  Copyright (c) 2022 James Hartman. All Rights Reserved
#  main.py
#  OrderBookFeed
#  Written by James Hartman <JamesLouisHartman@gmail.com.au>
#  Last modified 24/4/22, 11:13 pm

# !/usr/bin/env python3

import argparse
import sys
from heapq import nlargest, nsmallest
from operator import itemgetter

# done: 1 input stream parsing
# done: 2 pass to def for messages
# done: 3 handle messages for add, update, delete and execute
# done: 4 keep track of order book
# done: 5 trigger print when change occurs in top n of buy or sell
# done: 6 cleanup, command line arguments
# TODO: 7 optional: optimise, brute force first
# TODO: 8 optional: do potential (possible) error handling
# TODO: 9 optional: build for continuous input, i.e. unending, true stream of data
# TODO: 10 optional: stream corruption checking

BUY_STR = 'B'
SELL_STR = 'S'


class OrderBook:

    def __init__(self, n):
        """OrderBook Initializer

        Args:
            n (int): number of levels to monitor
        """
        # While this may seem convoluted, this method of referencing dicts is reportedly, on average, O(1) (very fast)
        self.symbols = {}  # {symbol: {B: {order_id: {"size": size, "price": price, "order_id": order_id}, ...}, ...}}
        self.levels = n
        self.snapshot = {}

    def add(self, symbol, side, order_id, price, size):
        """Creates a new order, errors if exists

        Args:
            symbol (str):
            side (str):
            order_id (int):
            price (int):
            size (int):

        Returns:

        """
        if symbol in self.symbols:
            if side in self.symbols[symbol]:
                if order_id in self.symbols[symbol][side]:
                    raise KeyError  # already exists, raise key error
                else:
                    self.symbols[symbol][side][order_id] = {"price": price, "size": size, "order_id": order_id}
            else:
                self.symbols[symbol][side] = {order_id: {"price": price, "size": size, "order_id": order_id}}
        else:
            self.symbols[symbol] = {side: {order_id: {"price": price, "size": size, "order_id": order_id}}}

    def update(self, symbol, side, order_id, price, size):
        """Updates an existing order, error if not exists

        Args:
            symbol (str):
            side (str):
            order_id (int):
            price (int):
            size (int):

        Returns:

        """
        self.symbols[symbol][side][order_id] = {"price": price, "size": size}

    def delete(self, symbol, side, order_id, size = None):
        """Deletes all or executes an amount from an order

        Args:
            symbol (str):
            side (str):
            order_id (int):
            size (int):

        Returns:

        """
        if size is None:
            # delete all
            del self.symbols[symbol][side][order_id]
        else:
            # execute some or all
            # below workaround needed as for some reason -= would modify the snapshot too
            _old_price = self.symbols[symbol][side][order_id]["price"]
            _new_size = self.symbols[symbol][side][order_id]["size"] - size
            self.update(symbol, side, order_id, _old_price, _new_size)
            if self.symbols[symbol][side][order_id]["size"] <= 0:
                del self.symbols[symbol][side][order_id]

    def take_action(self, action, sequence_num):
        """Executes an action

        Args:
            action (dict):
            sequence_num (int):

        Returns:

        """
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
        """Compares the incoming potential snapshot part against the matching existing snapshot part

        Args:
            symbol (str):
            side (str):
            sequence_num (int):

        Returns:

        """
        # for buy or sell in symbol (based on side): get n highest/lowest orders (based on price)
        _n = self.levels
        if len(self.symbols[symbol][side]) > 1:
            if side == BUY_STR:
                # highest_buys
                update = nlargest(_n, list(self.symbols[symbol][side].values()), key = itemgetter('price'))
            else:
                # lowest sells
                update = nsmallest(_n, list(self.symbols[symbol][side].values()), key = itemgetter('price'))
        elif len(self.symbols[symbol][side]) == 1:
            # save time if 1, since a list of length 1 will always be "ordered"
            update = list(self.symbols[symbol][side].values())
        else:
            update = []  # save time if blank

        if symbol in self.snapshot and side in self.snapshot[symbol]:
            if update != self.snapshot[symbol][side]:
                self.update_snapshot(symbol, side, update)
                self.print_snapshot(symbol, sequence_num)
        else:
            # new symbol, therefore it must be new
            self.update_snapshot(symbol, side, update)
            self.print_snapshot(symbol, sequence_num)

    def update_snapshot(self, symbol, side, update):
        """Updates a specific snapshot part

        Args:
            symbol (str):
            side (str):
            update (list):

        Returns:

        """
        if symbol in self.snapshot:
            self.snapshot[symbol][side] = update
        else:
            self.snapshot[symbol] = {side: update}

    def print_snapshot(self, symbol, sequence_num):
        """Builds and prints the snapshot string

        Args:
            symbol (str):
            sequence_num (int):

        Returns:

        """
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
    """slices the next n bytes (indexes)

    Args:
        mutable_bytes (bytearray):
        num_bytes (int):

    Returns:
        bytearray, bytearray
    """
    # According to profiling, this takes up 50% of execute time: called 318555 times with input2.stream
    # Threading is not an option, assuming that this program is designed for a "stream" of data (order important)
    read_bytes: bytearray = mutable_bytes[:num_bytes]
    remaining_bytes: bytearray = mutable_bytes[num_bytes:]
    return remaining_bytes, read_bytes


def parse_msg(mutable_bytes):
    """parses a bytes message, as defined in PDF, returning an action, reoresented as a dict

    Args:
        mutable_bytes (bytearray):

    Returns:
        dict
    """
    # shared by all: type, symbol, orderID, side, (padding)
    # TODO: look into 'import struct' for easier byte reading?
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


def main(stream, n):
    order_book = OrderBook(n)
    while len(stream) > 8:  # Any smaller value would not be able to fit a header.
        # This minimum length could be larger, depending on the absolute minimum size of a message:
        # Theoretically zero. However, the minimum length for any MEANINGFUL message is 13 (16 with padding)
        # Note: doing some waiting magic, it could be possible to use an unending stream, at variable rates
        stream, seq_bytes = read_n_bytes(stream, 4)
        stream, size_bytes = read_n_bytes(stream, 4)

        seq_num = int.from_bytes(seq_bytes, byteorder = 'little', signed = False)
        msg_size = int.from_bytes(size_bytes, byteorder = 'little', signed = False)

        stream, msg_bytes = read_n_bytes(stream, msg_size)
        order_book.take_action(parse_msg(msg_bytes), seq_num)


if __name__ == '__main__':
    desc_text_1 = 'Processes an input stream of order book messages, output a price depth'
    desc_text_2 = ' snapshot of the top N levels each time there is a visible change'
    parser = argparse.ArgumentParser(description = desc_text_1 + desc_text_2)
    # parser.add_argument("filePath", type = str, help = "File path of the binary file")
    parser.add_argument("levels", type = int, help = "how many levels to monitor for changes")
    args = parser.parse_args()
    # this is useful if it's easier to just give a path: byte_stream = open(args.filePath, "rb").read()

    # Closed: bug - corrupts when piping as cat file.stream | program.py (adds bytes and/or removes bytes)
    # note: may be environment-caused bug, honestly could be on of a lot of things...
    # update: for sanity, piped data directly within IDE, surprisingly works.
    # More research: https://github.com/PowerShell/PowerShell/issues/1908
    byte_stream = sys.stdin.buffer.read()
    mutable_byte_stream = bytearray(byte_stream)
    levels = args.levels

    main(mutable_byte_stream, levels)
