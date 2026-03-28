import os
import sys

__here__ = os.path.dirname(__file__)
sys.path.insert(0, __here__)

try:
    from .nkv_parser import parse, add, tsplit, split
except ImportError:
    try:
        from .nkv_parser import parse
    except ImportError as e:
        print(f"Erro importando módulo C++: {e}")
        print(f"Diretório atual: {__here__}")
        print(f"Arquivos disponíveis: {os.listdir(__here__)}")
        raise

import nkv.nkv
from nkv.nkv import NKVManager