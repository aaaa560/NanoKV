import ast
import json
import os
from batata import FileManager, ParamError, err
from typing import Any


class NKVManager(FileManager):
    """Nano Key-Value storage - Simples e Rápido"""
    SEPS: list[str] = ['|', '/', '\\', ' ', '-']
    DEC_TYPES: list[str] = ['[', '{', '(', ')', '}', ']']

    def __init__(self, name: str, path: str = './', sep_type: str = '|'):
        if '.' in name:
            name = name.split('.')[0] + '.nkv'
        elif not name.endswith('.nkv'):
            name += '.nkv'

        super().__init__(name=name, path=path)
        if not self.path.exists():
            os.mkdir(self.path)

        if sep_type not in self.SEPS:
            raise ParamError(
                message='\033[1;31mERRO! \033[1;34mParametro "sep_type" invalido!',
                param='sep_type',
                esperado=' | '.join(self.SEPS)
            )

        self.sep_type = sep_type

        if path.endswith('/'):
            self.arquivo = f'{path}{name}'
        else:
            self.arquivo = f'{path}/{name}'

    def write(self, key: str, value: Any) -> None:
        try:
            with open(self.arquivo, 'a', encoding='utf-8') as file:
                if type(value) == str:
                    file.write(f'{key}{self.sep_type}{type(value).__name__}:"{value}"\n')
                    return
                elif isinstance(value, bool):
                    line = f'{key}{self.sep_type}{type(value).__name__}:{str(value).lower()}\n'
                    file.write(line)
                    return
                file.write(f'{key}{self.sep_type}{type(value).__name__}:{value}\n')
        except FileNotFoundError:
            self.creat()
            with open(self.arquivo, 'a', encoding='utf-8') as file:
                file.write(f'{key}{self.sep_type}{type(value).__name__}:{value}\n')

    def write_decorator(self, decorator: str, tipe: str = '[') -> None:
        if tipe not in self.DEC_TYPES:
            raise ParamError(
                message='\033[1;31mParametro "tipe" invalido',
                param='tipe',
                esperado=' | '.join(self.DEC_TYPES)
            )
        ldec, rdec = '', ''

        match tipe:
            case '[':
                ldec, rdec = '[', ']'
            case '{':
                ldec, rdec = '{', '}'
            case '(':
                ldec, rdec = '(', ')'
            case _:
                raise ParamError(
                    message='\033[1;31mParametro "tipe" invalido',
                    param='tipe',
                    esperado=' | '.join(self.DEC_TYPES)
                )

        with open(self.arquivo, 'a', encoding='utf-8') as file:
            file.write(f'{ldec}{decorator}{rdec}\n')

    def no_typed_write(self, key: str, value: Any) -> None:
        try:
            with open(self.arquivo, 'a', encoding='utf-8') as file:
                if type(value) == str:
                    file.write(f'{key}{self.sep_type}"{value}"\n')
                    return
                file.write(f'{key}{self.sep_type}{value}\n')
        except FileNotFoundError:
            self.creat()
            with open(self.arquivo, 'a', encoding='utf-8') as file:
                file.write(f'{key}{self.sep_type}{value}\n')

    def read(self, decs: bool = False) -> dict[str, Any]:
        TYPE_MAP: dict[str, Any] = {
            'str': lambda x: x,
            'int': int,
            'float': float,
            'bool': lambda x: x.lower() == 'true',
            'list': ast.literal_eval,
            'dict': ast.literal_eval,
            'tuple': ast.literal_eval,
            'nonetype': lambda x: None
        }

        try:
            brute = self._get_data()
        except FileNotFoundError:
            print('\033[1;31mArquivo não encontrado! Criando novo arquivo...')
            self.creat()
            return {}

        content: dict[str, Any] = {}
        lines = brute.splitlines()

        for linha in lines:
            linha = linha.strip()
            if not linha:
                continue

            if '#' in linha:
                linha = self._strip_comment(linha)
                if not linha:
                    continue

            parts = linha.split(self.sep_type, 1)
            if len(parts) != 2:
                continue

            key = parts[0].strip()
            val_part = parts[1].strip()
            if ':' in val_part and not (val_part.startswith('"') and val_part.endswith('"')):

                tipo, raw_val = val_part.split(':', 1)
                tipo = tipo.strip().lower()
                raw_val = raw_val.strip()

                if raw_val.startswith('"') and raw_val.endswith('"'):
                    raw_val = raw_val[1:-1]

                converter = TYPE_MAP.get(tipo)

                try:
                    if converter:
                        parsed = converter(raw_val)
                    else:
                        parsed = raw_val
                except (ValueError, SyntaxError):
                    parsed = raw_val
            else:
                if val_part.startswith('"') and val_part.endswith('"'):
                    parsed = val_part[1:-1]
                elif val_part.lower() in ('true', 'false'):
                    parsed = True if val_part.lower() == 'true' else False
                else:
                    try:
                        if '.' in val_part:
                            parsed = float(val_part)
                        else:
                            parsed = int(val_part)
                    except ValueError:
                        parsed = val_part

            content[key] = parsed

        return content

    def nkv2json(self) -> dict:
        result: dict = {}
        this_list = None
        buffer: list = []

        for line in self._get_data().splitlines():
            line = line.strip()
            if not line:
                continue

            if line.startswith('[') and line.endswith(']'):
                if this_list is not None:
                    result[this_list] = buffer

                this_list = line[1:-1]
                buffer = []
                continue

            if self.sep_type in line:
                _, val = line.split(self.sep_type, 1)
                if val.startswith('str:"') and val.endswith('"'):
                    val = val[5:-1]

                    buffer.append(val)

        if this_list is not None:
            result[this_list] = buffer

        return result

    def get_sep(self) -> str:
        separator: str

        with open(self.arquivo, 'r', encoding='utf-8') as file:
            content = file.read()

        for sep in self.SEPS:
            for char in content:
                if char == sep:
                    return sep

        raise ValueError

    def find(self, name: str) -> dict[str, Any]:
        data: dict[str, Any] = self.read()
        result: dict[str, Any] = {}

        for key in data:
            if key == name:
                result[key] = data[key]
                return result
        return {}

    def get(self, key: str, default: Any = None) -> Any:
        data: dict[str, Any] = self.read()
        return data.get(key, default)

    def update(self, key: str, value: Any) -> bool:
        data = self.read()

        if key not in data:
            return False

        data[key] = value
        self._rewrite(data)
        return True

    def update_batch(self, new_data: dict[str, Any]) -> None:
        old_data = self.read()
        for key, value in new_data.items():
            if key not in old_data:
                continue
            old_data[key] = value

        self._rewrite(old_data)


    def delete(self, key: str) -> bool:
        data = self.read()

        if key not in data:
            return False

        del data[key]

        self._rewrite(data)

        return True

    def write_batch(self, data: dict[str, Any] | list[dict[str, Any]], beauty: bool = False) -> None:
        """
        Escreve múltiplos valores de uma vez (otimizado para abundância de dados)
        """
        with open(self.arquivo, 'a', encoding='utf-8') as file:
            if isinstance(data, dict):
                for key, value in data.items():
                    tipo = type(value).__name__

                    if isinstance(value, str):
                        file.write(f'{key}{self.sep_type}{tipo}:"{value}"\n')
                    elif isinstance(value, bool):
                        file.write(f'{key}{self.sep_type}{tipo}:{str(value).lower()}\n')
                    else:
                        file.write(f'{key}{self.sep_type}{tipo}:{value}\n')
            elif isinstance(data, list):
                for obj in data:
                    try:
                        for key, value in obj.items():
                            tipo = type(value).__name__

                            if isinstance(value, str):
                                file.write(f'{key}{self.sep_type}{tipo}:"{value}"\n')
                            elif isinstance(value, bool):
                                file.write(f'{key}{self.sep_type}{tipo}:{str(value).lower()}\n')
                            else:
                                file.write(f'{key}{self.sep_type}{tipo}:{value}\n')
                        if beauty: file.write('\n')
                    except Exception as e:
                        err(f'\033[1;34m{e}')

    def jsonify(self, indent: int = 2) -> str:
        """
        Converte NKV para JSON string
        ✅ Já funciona perfeitamente!
        """
        data = self.read()
        return json.dumps(data, indent=indent, ensure_ascii=False)

    def to_json_file(self, json_path: str, indent: int = 2) -> None:
        """
        Converte NKV para arquivo JSON
        ✅ Já funciona perfeitamente!
        """
        data = self.read()
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=indent, ensure_ascii=False)

    def nkvify(self, json_file: str, typed: bool = True) -> str:
        """
        Converte JSON para formato NKV (string)

        Args:
            json_file: Caminho do arquivo JSON
            typed: Se True, adiciona tipo explícito (tipo:valor)

        Returns:
            String no formato NKV

        Exemplo:
            nkv = NKVManager('config.nkv')
            nkv_str = nkv.nkvify('config.json')
            print(nkv_str)
        """
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)

        lines = []
        for key, value in data.items():
            line = self._format_nkv_line(key, value, typed)
            lines.append(line)

        return '\n'.join(lines)

    def from_json_file(self, json_file: str, typed: bool = True, beauty: bool = False) -> None:
        """
        Converte JSON file e ESCREVE no arquivo NKV

        Args:
            json_file: Caminho do arquivo JSON
            typed: Se True, usa tipagem explícita
            beauty: Deixa um pouco menos feiobm

        Exemplo:
            nkv = NKVManager('config.nkv')
            nkv.from_json_file('config.json')
            # Agora config.nkv tem o conteúdo do JSON
        """
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)

        with open(self.arquivo, 'w', encoding='utf-8') as f:
            f.write('')

        if typed:
            self.write_batch(data, beauty=beauty)
        else:
            with open(self.arquivo, 'w', encoding='utf-8') as f:
                for key, value in data.items():
                    line = self._format_nkv_line(key, value, typed=False)
                    f.write(line + '\n')

    @staticmethod
    def json_to_nkv_file(json_path: str, nkv_path: str, sep: str = '|', typed: bool = True) -> 'NKVManager':
        """
        Converte JSON file para NKV file (modo estático)

        Args:
            json_path: Caminho do JSON de entrada
            nkv_path: Caminho do NKV de saída
            sep: Separador NKV
            typed: Se True, usa tipagem explícita

        Returns:
            NKVManager instance

        Exemplo:
            NKVManager.json_to_nkv_file('config.json', 'config.nkv')
        """
        nkv = NKVManager(name=nkv_path, sep_type=sep)
        nkv.from_json_file(json_path, typed=typed)
        return nkv

    @staticmethod
    def _strip_comment(linha: str) -> str:
        in_str: bool = False
        result: str = ''

        for char in linha:
            if char == '"':
                in_str = not in_str
            if char == '#' and not in_str:
                break
            result += char

        return result

    def _format_nkv_line(self, key: str, value: Any, typed: bool) -> str:
        """
        Formata uma linha NKV com ou sem tipo

        Args:
            key: chave
            value: valor
            typed: se True, adiciona tipo explícito

        Returns:
            Linha formatada
        """
        tipo = type(value).__name__

        if typed:
            if isinstance(value, str):
                return f'{key}{self.sep_type}{tipo}:"{value}"'
            elif isinstance(value, bool):
                return f'{key}{self.sep_type}{tipo}:{str(value).lower()}'
            elif isinstance(value, (list, dict, tuple)):
                return f'{key}{self.sep_type}{tipo}:{json.dumps(value)}'
            elif value is None:
                return f'{key}{self.sep_type}nonetype:None'
            else:
                return f'{key}{self.sep_type}{tipo}:{value}'
        else:
            if isinstance(value, str):
                return f'{key}{self.sep_type}"{value}"'
            elif isinstance(value, bool):
                return f'{key}{self.sep_type}{str(value).lower()}'
            elif isinstance(value, (list, dict, tuple)):
                return f'{key}{self.sep_type}{json.dumps(value)}'
            else:
                return f'{key}{self.sep_type}{value}'

    @staticmethod
    def _find_couchettes(data: str) -> tuple[int, int]:
        return data.find('['), data.find(']') + 1

    def _rewrite(self, data: dict[str, Any]) -> None:
        with open(self.arquivo, 'w', encoding='utf-8') as file:
            for key, val in data.items():
                if isinstance(val, str):
                    file.write(f'{key}{self.sep_type}{type(val).__name__}:"{val}"\n')
                else:
                    file.write(f'{key}{self.sep_type}{type(val).__name__}:{val}\n')

    def _rewrite_legacy(self, data: dict[str, Any]) -> None:
        with open(self.arquivo, 'w', encoding='utf-8') as file:
            for key, val in data.items():
                if isinstance(val, str):
                    file.write(f'{key}{self.sep_type}"{val}"\n')
                else:
                    file.write(f'{key}{self.sep_type}{val}\n')

    def _get_data(self) -> str:
        with open(self.arquivo, 'r', encoding='utf-8') as file:
            return file.read()
