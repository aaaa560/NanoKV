from nkv import NKVManager

nkv = NKVManager('testes', './tests')

nkv.update_batch({
    'a': 123,
    'x': 'teste2'
})
