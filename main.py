#  main.py
#  OrderBookFeed
#
#  Written by James Hartman <JamesLouisHartman@gmail.com.au>
#  Last modified 22/4/22, 4:24 pm
#  Copyright (c) 2022 James Hartman. All Rights Reserved

#  main.py
#  OrderBookFeed
#
#  main.py
#  OrderBookFeed
#
#  main.py
#  OrderBookFeed
#

# done: input stream parsing
# TODO: pass to def for messages
# TODO: handle messages for add, update, delete and execute
# TODO: keep track of order book, trigger print when change occurs in top n of buy or sell
# TODO: do potential (possible) error handling
# TODO: cleanup
# TODO: optional: build for continuous input, i.e. unending, true stream of data
# TODO: optional: stream corruption checking

import sys


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
    #


if __name__ == '__main__':
    # parse command line
    foo = sys.stdin.buffer.read()
    # todo: add input for top N levels
    mutable_foo = bytearray(foo)

    # we always start with a header of 4 bytes
    while len(mutable_foo) > 8:  # any smaller value would not be able to fit a header (see note 1)
        mutable_foo, seq_bytes = read_n_bytes(mutable_foo, 4)
        mutable_foo, size_bytes = read_n_bytes(mutable_foo, 4)
        seq_num = int.from_bytes(seq_bytes, byteorder = 'little', signed = False)
        msg_size = int.from_bytes(size_bytes, byteorder = 'little', signed = False)
        mutable_foo, msg_bytes = read_n_bytes(mutable_foo, msg_size)
        parse_msg(msg_bytes)
        print(f'seq: {seq_num}, size: {msg_size}, msg: {msg_bytes.hex(bytes_per_sep = 4, sep = " ")}')

# note 1: this value could be larger, depending on the absolute minimum size of a message (theoretically zero)
