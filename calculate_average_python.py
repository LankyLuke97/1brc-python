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

def worker(mm: Any, ptr: int, end: int, process_queue: Queue) -> None:
    results = {}
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
        if station not in results: results[station] = [100,0,-100,0]
        prev = results[station]
        if value < prev[0]: prev[0] = value
        if value > prev[2]: prev[2] = value
        prev[1] += value
        prev[3] += 1
    process_queue.put(results)

def combine(process_queue: Queue, num_blocks: int) -> Dict[str, tuple[float, float, float, int]]:
    combined_results = {}
    sorted_keys = []
    num_blocks -= 1
    while num_blocks:
        num_blocks -= 1
        result = process_queue.get()
        for station, (min_, total, max_, num) in result.items():
            if station not in combined_results: 
                combined_results[station] = [100,0,-100,0]
                insort(sorted_keys, station)
            prev = combined_results[station]
            if min_ < prev[0]: prev[0] = min_
            if max_ > prev[2]: prev[2] = max_
            prev[1] += total
            prev[3] += num
    return sorted_keys, combined_results


profile_out = input('Please provide an output filename for the profiler: ')

with cProfile.Profile() as profile:
    start = perf_counter()
    stations = {}
    i = 0
    block_size = 1024

    input_file = Path('measurements.txt')
    total_bytes = input_file.stat().st_size
    blocks = total_bytes / block_size
    if int(blocks) != blocks: blocks = int(blocks) + 1
    else: blocks = int(blocks)

    with input_file.open(mode='rb') as file:
        mm = mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ)
        process_queue = Queue()
        stations = sorted_keys = None
        futures = []
        i = 0

        with ThreadPoolExecutor() as executor:
            future = executor.submit(combine, process_queue, blocks)
            futures.append(future)
            for block in range(blocks): futures.append(executor.submit(worker, mm, block * block_size, (block+1) * block_size, process_queue))
            wait(futures)
            # for f in as_completed(futures):
            #     i += 1
            #     if i % 1_000 == 0: print(f'Processed {(i*block_size) // 1024} KB out of {total_bytes // 1024}')
            
            sorted_keys, stations = future.result()

    with open('calculated_python.txt', mode='w', encoding='utf-8') as out_file:
        out_file.write('{') 
        out_file.write(', '.join([f'{str(k)}={stations[k][0]}/{round(stations[k][1] / stations[k][3], 1)}/{stations[k][2]}' for k in sorted_keys]))
        out_file.write('}') 
        

    end = perf_counter()
    print(f'Script complete in {end-start}')
    profile.dump_stats(f"{profile_out.removesuffix('.prof')}.prof")
