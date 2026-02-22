from bisect import insort
import cProfile 
from collections import defaultdict
import mmap
from time import perf_counter
from concurrent.futures import ThreadPoolExecutor, as_completed, wait
from pathlib import Path
from queue import Queue

SEP = ord(';')
END = ord('\n')

def worker(mm: Any, ptr: int, end: int, results: Dict[str, List[float, float, float, int]], sorted_keys: List[str]) -> None:
    if ptr != 0:
        while mm[ptr] != END: ptr += 1
        ptr += 1
    while mm[end] != END: end += 1
    while ptr < end:
        seg_start = ptr
        while mm[ptr] != SEP: ptr += 1
        station = mm[seg_start:ptr]
        ptr += 1
        seg_start = ptr
        while mm[ptr] != END: ptr += 1
        value = float(mm[seg_start:ptr])
        ptr += 1
        if station not in results:
            results[station] = [100,0,-100,0]
            insort(sorted_keys, station)
        prev = results[station]
        if value < prev[0]: prev[0] = value
        if value > prev[2]: prev[2] = value
        prev[1] += value
        prev[3] += 1

profile_out = input('Please provide an output filename for the profiler: ')

with cProfile.Profile() as profile:
    start = perf_counter()
    stations = {}
    i = 0
    block_size = 1024

    input_file = Path('measurements_short.txt')
    total_bytes = input_file.stat().st_size
    blocks = total_bytes / block_size
    if int(blocks) != blocks: blocks = int(blocks) + 1
    else: blocks = int(blocks)

    with input_file.open(mode='rb') as file:
        mm = mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ)
        stations = {}
        sorted_keys = [] 
        futures = []
        i = 0

        with ThreadPoolExecutor() as executor:
            for block in range(blocks): futures.append(executor.submit(worker, mm, block * block_size, (block+1) * block_size, stations, sorted_keys))
            wait(futures)
            # for f in as_completed(futures):
            #     i += 1
            #     if i % 1_000 == 0: print(f'Processed {(i*block_size) // 1024} KB out of {total_bytes // 1024}')
            

    print(len(stations))
    with open('calculated_python.txt', mode='w', encoding='utf-8') as out_file:
        out_file.write('{') 
        out_file.write(', '.join([f'{str(k)}={stations[k][0]}/{round(stations[k][1] / stations[k][3], 1)}/{stations[k][2]}' for k in sorted_keys]))
        out_file.write('}') 
        

    end = perf_counter()
    print(f'Script complete in {end-start}')
    profile.dump_stats(f"{profile_out.removesuffix('.prof')}.prof")
