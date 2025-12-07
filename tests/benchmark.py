# ===============================================================
#    This code in specific has mabe by an AI (Gemini)
# ===============================================================


import time
import json
import csv
import statistics  # NOVO! Para calcular média e desvio padrão
import ast  # Para o parsing otimizado de lista/dict/tuple na leitura
from pathlib import Path
from typing import Any, Callable, Dict, Tuple

# Importa a sua biblioteca (ajuste o caminho se necessário)
# Assumindo que 'err' é uma função de logging ou print de erro simples,
# e que 'FileManager' e 'ParamError' estão definidos na sua 'batata-lib'.
from batata.files import NKVManager, FileManager, ParamError


def timeit_consistent(func: Callable, *args,
                      runs: int = 10,
                      **kwargs) -> tuple[Any, float, float]:  # Retorna resultado, média, desvio padrão
    """
    Mede tempo de execução de função com múltiplas rodadas para consistência.
    Retorna resultado, tempo médio (ms) e desvio padrão (ms).
    """
    times = []

    # 1. Aquecimento (Warm-up): Executa uma vez para aquecer caches e JIT (não conta o tempo)
    try:
        result = func(*args, **kwargs)
    except Exception as e:
        print(f"Erro na fase de aquecimento: {e}")
        raise

    # 2. Executar múltiplas vezes para calcular média
    for _ in range(runs):
        start = time.perf_counter()
        func(*args, **kwargs)
        end = time.perf_counter()
        # Tempo em milissegundos
        times.append((end - start) * 1000)

    # 3. Calcular Média e Desvio Padrão
    mean_time = statistics.mean(times)

    try:
        stdev = statistics.stdev(times)
    except statistics.StatisticsError:
        stdev = 0.0

    # Retorna o resultado da última execução, o tempo médio e o desvio padrão.
    return result, mean_time, stdev


def generate_test_data(size: int) -> Dict[str, Any]:
    """Gera dados de teste variados"""
    data: Dict[str, Any] = {}
    for i in range(size):
        data[f'str_{i}'] = f'value_{i}'
        data[f'int_{i}'] = i
        data[f'float_{i}'] = i * 1.5
        data[f'bool_{i}'] = i % 2 == 0
    return data


### NKV

def run_nkv_write(data: Dict[str, Any], filepath: str) -> Tuple[float, float]:
    """Benchmark velocidade de escrita NKV usando write_batch (otimizado)"""
    Path(filepath).unlink(missing_ok=True)

    nkv = NKVManager(name=Path(filepath).name, path=str(Path(filepath).parent or './'))

    # Mede o tempo com múltiplas rodadas
    _, mean_time, stdev = timeit_consistent(nkv.write_batch, data, runs=10)
    return mean_time, stdev


def run_nkv_read(filepath: str) -> Tuple[Dict, float, float]:
    """Benchmark velocidade de leitura NKV (Otimizada)"""
    nkv = NKVManager(name=Path(filepath).name, path=str(Path(filepath).parent or './'))
    # Mede o tempo com múltiplas rodadas
    data, mean_time, stdev = timeit_consistent(nkv.read, runs=10)
    return data, mean_time, stdev


### JSON

def run_json_write(data: Dict[str, Any], filepath: str) -> Tuple[float, float]:
    """Benchmark velocidade de escrita JSON"""

    def write_json():
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f)

    _, mean_time, stdev = timeit_consistent(write_json, runs=10)
    return mean_time, stdev


def run_json_read(filepath: str) -> Tuple[Dict, float, float]:
    """Benchmark velocidade de leitura JSON"""

    def read_json():
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)

    data, mean_time, stdev = timeit_consistent(read_json, runs=10)
    return data, mean_time, stdev


### CSV

def run_csv_write(data: Dict[str, Any], filepath: str) -> Tuple[float, float]:
    """Benchmark velocidade de escrita CSV"""

    def write_csv():
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            # CSV precisa de um header se for ser lido por DictReader
            writer.writerow(['key', 'type', 'value'])
            for key, value in data.items():
                writer.writerow([key, type(value).__name__, value])

    _, mean_time, stdev = timeit_consistent(write_csv, runs=10)
    return mean_time, stdev


def run_csv_read(filepath: str) -> Tuple[Dict, float, float]:
    """Benchmark velocidade de leitura CSV"""

    def read_csv():
        result = {}
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            # A leitura CSV é lenta pois itera sobre o leitor
            for row in reader:
                result[row['key']] = row['value']
        return result

    data, mean_time, stdev = timeit_consistent(read_csv, runs=10)
    return data, mean_time, stdev


def stress_test(sizes: list[int]) -> dict:
    """Testa performance com diferentes tamanhos de dados"""
    results = {
        'NKV': {'write': [], 'read': [], 'total': []},
        'JSON': {'write': [], 'read': [], 'total': []},
        'CSV': {'write': [], 'read': [], 'total': []}
    }

    nkv_file = 'benchmark.nkv'
    json_file = 'benchmark.json'
    csv_file = 'benchmark.csv'

    for size in sizes:
        print(f"\n{'=' * 60}")
        print(f"🔥 Testando com {size} keys ({size * 4} valores totais) - Média de 10 rodadas")
        print(f"{'=' * 60}")

        data = generate_test_data(size)

        # 1. NKV
        print("📝 Testando NKV...")
        nkv_write_time, nkv_write_stdev = run_nkv_write(data, nkv_file)
        _, nkv_read_time, nkv_read_stdev = run_nkv_read(nkv_file)

        nkv_total = nkv_write_time + nkv_read_time
        results['NKV']['write'].append(nkv_write_time)
        results['NKV']['read'].append(nkv_read_time)
        results['NKV']['total'].append(nkv_total)

        # 2. JSON
        print("📝 Testando JSON...")
        json_write_time, json_write_stdev = run_json_write(data, json_file)
        _, json_read_time, json_read_stdev = run_json_read(json_file)

        json_total = json_write_time + json_read_time
        results['JSON']['write'].append(json_write_time)
        results['JSON']['read'].append(json_read_time)
        results['JSON']['total'].append(json_total)

        # 3. CSV
        print("📝 Testando CSV...")
        csv_write_time, csv_write_stdev = run_csv_write(data, csv_file)
        _, csv_read_time, csv_read_stdev = run_csv_read(csv_file)

        csv_total = csv_write_time + csv_read_time
        results['CSV']['write'].append(csv_write_time)
        results['CSV']['read'].append(csv_read_time)
        results['CSV']['total'].append(csv_total)

        # Exibe resultados
        print(f"\n📊 RESULTADOS MÉDIOS ({size} keys):")

        # Tabela 1: Tempos Médios
        print(f"\n{'Formato':<10} {'Escrita':>12} {'Leitura':>12} {'Total':>12}")
        print(f"{'-' * 50}")
        print(f"{'NKV':<10} {nkv_write_time:>10.2f}ms {nkv_read_time:>10.2f}ms {nkv_total:>10.2f}ms")
        print(f"{'JSON':<10} {json_write_time:>10.2f}ms {json_read_time:>10.2f}ms {json_total:>10.2f}ms")
        print(f"{'CSV':<10} {csv_write_time:>10.2f}ms {csv_read_time:>10.2f}ms {csv_total:>10.2f}ms")

        # Tabela 2: Consistência (Desvio Padrão)
        print(f"\n{'Consistência (StDev)':<20} {'Escrita':>12} {'Leitura':>12}")
        print(f"{'-' * 45}")
        print(f"{'NKV StDev':<20} {nkv_write_stdev:>10.2f}ms {nkv_read_stdev:>10.2f}ms")
        print(f"{'JSON StDev':<20} {json_write_stdev:>10.2f}ms {json_read_stdev:>10.2f}ms")
        print(f"{'CSV StDev':<20} {csv_write_stdev:>10.2f}ms {csv_read_stdev:>10.2f}ms")

        # Calcula vencedor
        if json_total < nkv_total:
            diff_percent = ((nkv_total - json_total) / json_total) * 100
            print(f"\n⚡ JSON é {diff_percent:.1f}% mais rápido que NKV")
        else:
            diff_percent = ((json_total - nkv_total) / nkv_total) * 100
            print(f"\n🔥 NKV é {diff_percent:.1f}% mais rápido que JSON!")

        times = {'NKV': nkv_total, 'JSON': json_total, 'CSV': csv_total}
        winner = min(times, key=times.get)
        print(f"🏆 VENCEDOR: {winner}")

    return results


def file_size_comparison(size: int) -> None:
    """Compara tamanho dos arquivos gerados"""
    print(f"\n{'=' * 60}")
    print(f"📦 ANÁLISE DE TAMANHO DE ARQUIVO ({size} keys)")
    print(f"{'=' * 60}")

    data = generate_test_data(size)

    nkv_size_file = 'size_test.nkv'
    json_size_file = 'size_test.json'
    csv_size_file = 'size_test.csv'

    # Gera arquivos
    run_nkv_write(data, nkv_size_file)
    run_json_write(data, json_size_file)
    run_csv_write(data, csv_size_file)

    # Mede tamanhos
    nkv_size = Path(nkv_size_file).stat().st_size
    json_size = Path(json_size_file).stat().st_size
    csv_size = Path(csv_size_file).stat().st_size

    print(f"\n{'Formato':<10} {'Bytes':>12} {'KB':>12}")
    print(f"{'-' * 36}")
    print(f"{'NKV':<10} {nkv_size:>12} {nkv_size / 1024:>11.2f}")
    print(f"{'JSON':<10} {json_size:>12} {json_size / 1024:>11.2f}")
    print(f"{'CSV':<10} {csv_size:>12} {csv_size / 1024:>11.2f}")

    # Compara
    smallest = min(nkv_size, json_size, csv_size)
    if nkv_size == smallest:
        winner = "NKV"
    elif json_size == smallest:
        winner = "JSON"
    else:
        winner = "CSV"

    print(f"\n🏆 MAIS COMPACTO: {winner}")

    # Mostra diferenças
    print(f"\nDiferenças em relação ao menor:")
    for name, size in [('NKV', nkv_size), ('JSON', json_size), ('CSV', csv_size)]:
        diff_percent = ((size - smallest) / smallest) * 100
        if size == smallest:
            print(f"  {name}: menor arquivo")
        else:
            print(f"  {name}: +{diff_percent:.1f}% maior")

    # Cleanup
    Path(nkv_size_file).unlink(missing_ok=True)
    Path(json_size_file).unlink(missing_ok=True)
    Path(csv_size_file).unlink(missing_ok=True)


def test_type_preservation() -> None:
    # ... (código inalterado)
    # A única função com 'test_' que mantive, pois ela é um teste unitário funcional
    # e não uma rotina do benchmark.

    print(f"\n{'=' * 60}")
    print("🔬 TESTE DE PRESERVAÇÃO DE TIPOS")
    print(f"{'=' * 60}")

    nkv = NKVManager('type_test.nkv')

    test_data = {
        'string': 'hello world',
        'integer': 42,
        'float': 3.14159,
        'bool_true': True,
        'bool_false': False,
        'negative': -100,
        'zero': 0
    }

    print("\n📝 Escrevendo dados...")
    for key, value in test_data.items():
        nkv.write(key, value)
        print(f"  {key}: {value} ({type(value).__name__})")

    print("\n📖 Lendo dados...")
    result = nkv.read()

    print("\n✅ Verificando tipos:")
    all_correct = True
    for key, expected_value in test_data.items():
        actual_value = result[key]
        actual_type = type(actual_value).__name__
        expected_type = type(expected_value).__name__

        type_match = actual_type == expected_type
        value_match = actual_value == expected_value

        status = "✅" if (type_match and value_match) else "❌"
        print(f"  {status} {key}: {actual_value} ({actual_type}) - expected {expected_type}")

        if not (type_match and value_match):
            all_correct = False

    if all_correct:
        print("\n🎉 Todos os tipos foram preservados corretamente!")
    else:
        print("\n⚠️  Alguns tipos não foram preservados")

    Path('type_test.nkv').unlink(missing_ok=True)


def main():
    print("=" * 60)
    print("🔥 BENCHMARK: NKV (Batch/Otimizado) vs JSON vs CSV")
    print("=" * 60)
    print("\nUsando média de 10 rodadas para maior consistência no MacBook Air M1...")

    # 1. Teste de preservação de tipos (se você o mantiver)
    # test_type_preservation()

    # 2. Stress test com diferentes tamanhos
    print(f"\n{'=' * 60}")
    print("⚡ STRESS TEST - Múltiplos tamanhos até 50K")
    print(f"{'=' * 60}")

    # NOVOS TAMANHOS: Para cobrir desde a pequena carga até a extrema
    sizes = [100, 1000, 5000, 10000, 20000, 50000]
    results = stress_test(sizes)

    # 3. Análise de tamanho de arquivo
    # file_size_comparison(50000)

    # 4. Resumo final
    print(f"\n{'=' * 60}")
    print("📊 RESUMO FINAL (Tempo Total Médio em ms)")
    print(f"{'=' * 60}")

    print(f"\n{'Keys':<10} {'NKV':>12} {'JSON':>12} {'CSV':>12}")
    print(f"{'-' * 50}")
    for size_idx, size in enumerate(sizes):
        nkv_total = results['NKV']['total'][size_idx]
        json_total = results['JSON']['total'][size_idx]
        csv_total = results['CSV']['total'][size_idx]

        print(f"{size:<10} {nkv_total:>10.2f}ms {json_total:>10.2f}ms {csv_total:>10.2f}ms")

    print("\n✅ Benchmark concluído! Verifique as colunas 'StDev' para a consistência.")


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Benchmark interrompido pelo usuário")
    except Exception as e:
        print(f"\n❌ Erro durante benchmark: {e}")
        import traceback

        traceback.print_exc()
