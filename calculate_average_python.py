from collections import defaultdict
from time import perf_counter


start = perf_counter()
stations = {}
i = 0

with open('measurements.txt',mode='r',encoding='utf-8') as file:
    for line in file:
        i += 1
        s,val = line.split(';')
        val = float(val)
        if s not in stations:
            stations[s] = [val, val, val, 1]
        else:
            stations[s][1] += val
            stations[s][3] += 1
            if val < stations[s][0]: stations[s][0] = val
            elif val > stations[s][2]: stations[s][2] = val
        if i % 1000000 == 0: print(f'Processed {i:,} lines')

alphabetical = sorted(stations.keys())
with open('calculated_python.txt', mode='w', encoding='utf-8') as out_file:
    out_file.write('{') 
    out_file.write(', '.join([f'{k}={stations[k][0]}/{round(stations[k][1] / stations[k][3], 1)}/{stations[k][2]}' for k in alphabetical]))
    out_file.write('}') 
    

end = perf_counter()
print(f'Script complete in {end-start}')
