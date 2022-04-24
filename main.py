#  Copyright (c) 2022 James Hartman. All Rights Reserved
#  main.py
#  OrderBookFeed
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
# TODO: 3 handle messages for add, update, delete and execute
# TODO: 4 keep track of order book, trigger print when change occurs in top n of buy or sell
# TODO: 5 do potential (possible) error handling
# TODO: 6 cleanup
# TODO: 7 optimise, brute force first
# TODO: 8 optional: build for continuous input, i.e. unending, true stream of data
# TODO: 9 optional: stream corruption checking


# order book = [symbols]
# book = {"symbol": {"buys": {"order_id": {"size": "size", "price": "price"}, ...}, ...}}
# buy/sell = {order_id: {size: size, price: price}, ...}

# order book = [symbols]
# symbols = {[buy], [sell]}
# buy/sell = id: {size: size, price: price}
# order = {id: [size, price], ...}


class OrderBook:

    def __init__(self):
        self.symbols = {}
        # {"symbol": {"buys": {"order_id": {"size": "size", "price": "price"}, ...}, ...}}

    def add(self, symbol, side, order_id, price, size):
        # create a new order, error if exists
        if symbol in self.symbols:
            if side in self.symbols[symbol]:
                if order_id in self.symbols[symbol][side]:
                    raise KeyError
                else:
                    self.symbols[symbol][side][order_id] = {"price": price, "size": size}
            else:
                self.symbols[symbol][side] = {order_id: {"price": price, "size": size}}
        else:
            self.symbols[symbol] = {side: {order_id: {"price": price, "size": size}}}

    def update(self, symbol, side, order_id, price, size):
        # update an existing order, error if not exists
        if order_id in self.symbols[symbol][side]:
            self.symbols[symbol][side][order_id] = {"price": price, "size": size}
        else:
            raise KeyError

    def delete(self, symbol, side, order_id, size = None):
        # delete all or execute an amount from an order
        if size is None:
            # delete all
            del self.symbols[symbol][side][order_id]
        else:
            # execute some or all
            self.symbols[symbol][side][order_id]["size"] -= size
            if self.symbols[symbol][side][order_id]["size"] <= 0:
                del self.symbols[symbol][side][order_id]

    def take_action(self, action):
        if action['type'] == 'A':
            # ADD an order
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
        raise ValueError  # TODO: refer to todo 9
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
    foo = open("input1.stream", "rb").read()
    print(type(foo))
    mutable_foo = bytearray(foo)

    # todo: make argument
    levels = 5

    order_book = OrderBook()

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
        order_book.take_action(parse_msg(msg_bytes))
