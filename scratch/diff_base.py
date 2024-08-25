import jsondiff as jd
from eth_loader.base_sql import BaseSQliteDB
import json
import difflib

a = {"1": "1", "2": 2, "3": True, "4": None, "5": [1, 2, 3], "6": {"a": 1, "b": 2}}
b = {"1": "1", "3": False, "4": None, "5": [1, 2, 4], "6": {"a": 2, "c": 4}}

# a is the first thing
# b is the second thing
simple_delta = jd.diff(a, b)

print("Simple Delta: ", simple_delta)

b_reconstructed = jd.patch(a, simple_delta)
# Doesn't work the following
# b_reconstructed = jd.patch(simple_delta, a)
print("Reconstructed == Actual: ", b_reconstructed == b)