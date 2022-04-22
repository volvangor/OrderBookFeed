#  main.py
#  OrderBookFeed
#
#  Written by James Hartman <JamesLouisHartman@gmail.com.au>
#  Last modified 22/4/22, 10:59 am
#  Copyright (c) 2022 James Hartman. All Rights Reserved

#  main.py
#  OrderBookFeed
#
import argparse


def init():
    parser = argparse.ArgumentParser()
    parser.add_argument("first", type = str, help = "first input", required = False)
    parser.add_argument("second", type = str, help = "second input", required = False)
    args = parser.parse_args()
    first, second = "", ""
    first, second = args.first, args.second
    print(f'first: {first}, second: {second}')


if __name__ == '__main__':
    init()

# TODO: input stream parsing
# TODO: pass to def for messages
# TODO: handle messages for add, update, delete and execute
