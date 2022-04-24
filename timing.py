#  Copyright (c) 2022 James Hartman. All Rights Reserved
#  timing.py
#  OrderBookFeed
#  Written by James Hartman <JamesLouisHartman@gmail.com.au>
#  Last modified 24/4/22, 10:43 pm

import timeit
from collections import deque

foo = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
foo1 = deque(foo)
bar1 = []
for _ in range(3):
    bar1.append(foo1.popleft())

foo2 = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
bar2 = foo2[:3]
foo2 = foo2[3:]

print(f'foo, bar')
print(f'{foo1}, {bar1}')
print(f'{foo2}, {bar2}')

setup_code = "from collections import deque; foo = deque(range(1000000))"
testcode = ''' 
def test(): 
    
    bar = []
    for _ in range(30000):
        bar.append(foo.popleft())
    
'''
print(f'deque: {timeit.timeit(stmt = testcode, setup = setup_code)}')

setup_code2 = "foo = range(1000000)"
testcode2 = ''' 
def test(): 

    bar = foo[:30000]
    foo = foo[30000:]

'''
print(f'slice: {timeit.timeit(stmt = testcode2, setup = setup_code2)}')


