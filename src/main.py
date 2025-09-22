import os
import sys
import json
import asyncio
from collections import Counter
from multiprocessing import Pool

INPUT_PATH = "data"
OUTPUT_PATH = "out"
NUM_PROCESSES = 4


def mapper(file_name):
    counter = Counter()
    print(f"processing file: {file_name}")
    file_path = os.path.join(INPUT_PATH, file_name)
    file_prefix = os.path.splitext(file_name)[0]
    with open(file_path, 'r') as f:
        lines = f.readlines()
        for line in lines:
            try:
                number = int(line.strip())
                counter[number] += 1
            except ValueError as e:
                print(f"Skipping invalid line: {line}")
                continue

    output_path = os.path.join(OUTPUT_PATH, f"{file_prefix}.json")
    with open(output_path, 'w') as f:
        json.dump(counter, f)

    print(f"saved file: {output_path}")



def reduce(files):
    result = Counter()
    for file in files:
        with open(os.path.join(OUTPUT_PATH, file)) as f:
            counter = Counter(json.load(f))
            result += counter

    out_path = 'result.json'
    top_six_numbers = result.most_common(6)
    print(f"top_six_numbers: {top_six_numbers}")
    top_six_result = {str(k): v for k, v in top_six_numbers}
    with open(out_path, 'w') as f:
        json.dump(top_six_result, f)

    print(f"Output saved to {out_path}")


def main():
    files = os.listdir(INPUT_PATH)
    # file_partitions = [files[i::NUM_PROCESSES] for i in range(NUM_PROCESSES)]

    with Pool(processes=NUM_PROCESSES) as pool:
        pool.map(mapper, files)

    out_files = os.listdir(OUTPUT_PATH)
    reduce(out_files)

    print('Completed execution')

if __name__ == "__main__":
    main()
    



