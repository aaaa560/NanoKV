from nkv import NKVManager, tsplit  # type: ignore


with open("tests.nkv", "r") as f:
    lines = f.readlines()
    for line in lines:
        print(tsplit(line, "|", ":"))