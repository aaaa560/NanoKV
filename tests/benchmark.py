import time
import timeit
import json
import gc
import statistics
from pathlib import Path
from nkv import NKVManager

ITERATIONS = 50  # Número de vezes para rodar cada teste
WARMUP = 5  # Rodadas para "aquecer" cache de disco/CPU


def setup_environment():
    if not Path('./tests.json').resolve().exists():
        nkv = NKVManager('tests.nkv', str(Path('./').resolve()))
        nkv.to_json_file(str(Path('./tests.json').resolve()))


def run_benchmark(label, func, *args):
    times = []

    # Warmup (ignorar tempos)
    for _ in range(WARMUP):
        func(*args)

    # Real Benchmark
    print(f"Running {label}...", end='', flush=True)
    for _ in range(ITERATIONS):
        # Desabilita GC para não afetar a medição de tempo puro de CPU/IO
        gc_old = gc.isenabled()
        gc.disable()

        start = time.perf_counter()  # Mais preciso que time.time()
        func(*args)
        end = time.perf_counter()

        if gc_old: gc.enable()
        times.append(end - start)

    print(f" Done.")
    return times


# Wrappers para as funções
def bench_python():
    path = str(Path('./').resolve())
    mgr = NKVManager(name='tests.nkv', path=path)
    mgr.read(big_data=False, c_parse=False)


def bench_cpp():
    path = str(Path('./').resolve())
    mgr = NKVManager(name='tests.nkv', path=path)
    mgr.read(c_parse=True)


def bench_json():
    with open('./tests.json', 'r') as file:
        json.load(file)


def print_stats(name, times):
    avg = statistics.mean(times) * 1000
    median = statistics.median(times) * 1000
    stdev = statistics.stdev(times) * 1000
    best = min(times) * 1000
    worst = max(times) * 1000

    print(f"--- {name} ---")
    print(f"  Média:   {avg:7.4f} ms")
    print(f"  Mediana: {median:7.4f} ms")
    print(f"  Melhor:  {best:7.4f} ms")
    print(f"  Pior:    {worst:7.4f} ms")
    print(f"  Desvio:  {stdev:7.4f} ms (instabilidade)")
    return avg


if __name__ == "__main__":
    setup_environment()
    print(f"Benchmark Configuration: {ITERATIONS} runs, {WARMUP} warmups on M1 Architecture\n")

    t_py = run_benchmark("Python NKV", bench_python)
    t_cpp = run_benchmark("C++ NKV", bench_cpp)
    t_json = run_benchmark("JSON", bench_json)

    avg_py = print_stats("Python (Pure)", t_py)
    avg_cpp = print_stats("C++ Extension", t_cpp)
    avg_json = print_stats("Standard JSON", t_json)

    print("\n--- Comparativo (Speedup) ---")
    print(f"C++ é {avg_py / avg_cpp:.2f}x mais rápido que Python")
    print(f"C++ é {avg_json / avg_cpp:.2f}x mais rápido que JSON")