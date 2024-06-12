import json
import glob
import csv

def process_files(file_name:str, process_entry, finalize_results, csv_headers, csv_file_name):
    sums = {}
    counts = {}

    results_file = '/'.join(__file__.split('/')[:-1]) if '/' in __file__ else '/'.join(__file__.split('\\')[:-1])
    file_pattern = f'{results_file}/../results/{file_name}/*.json'
    print(file_pattern)

    for file_name in glob.glob(file_pattern):
        with open(file_name, 'r') as file:
            data = json.load(file)
            for entry in data:
                process_entry(entry, sums, counts, file_name)

    results = finalize_results(sums, counts)

    csv_file_path = f'{results_file}/../results/{csv_file_name}'
    with open(csv_file_path, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(csv_headers)
        for result in results:
            writer.writerow(result)

    print(f"{csv_file_name} written to {csv_file_path}")

def calc_averages(file_name:str):
    def process_entry(entry, sums, counts, file_name):
        engine = entry['engine']
        df_type = entry['df_type']
        key = (engine, df_type)

        if key not in sums:
            sums[key] = {metric: 0 for metric in entry['results']}
            counts[key] = {metric: 0 for metric in entry['results']}

        for metric, value in entry['results'].items():
            try:
                numeric_value = float(value)
                sums[key][metric] += numeric_value
                counts[key][metric] += 1
            except ValueError:
                print(f"Warning: Skipping non-numeric value for {metric} in {file_name}")

    def finalize_results(sums, counts):
        averages = []
        for key, metrics in sums.items():
            for metric, sum_value in metrics.items():
                averages.append([key[0], key[1], metric, sum_value / counts[key][metric]])
        return averages

    process_files(file_name, process_entry, finalize_results, ['Engine', 'DF_Type', 'Metric', 'Average Value'], 'averages.csv')

def calc_best_averages(file_name:str):
    def process_entry(entry, sums, counts, file_name):
        engine = entry['engine']
        df_type = entry['df_type']
        key = (engine, df_type)
        for metric, value in entry['results'].items():
            try:
                numeric_value = float(value)
                if metric not in sums:
                    sums[metric] = {}
                    counts[metric] = {}
                if key not in sums[metric]:
                    sums[metric][key] = 0
                    counts[metric][key] = 0
                sums[metric][key] += numeric_value
                counts[metric][key] += 1
            except ValueError:
                print(f"Warning: Skipping non-numeric value for {metric} in {file_name}")

    def finalize_results(sums, counts):
        best_averages = []
        for metric, keys in sums.items():
            best_average = None
            for key, sum_value in keys.items():
                average = sum_value / counts[metric][key]
                if best_average is None or average < best_average['value']:
                    best_average = {'value': average, 'engine': key[0], 'df_type': key[1]}
            best_averages.append([metric, best_average['engine'], best_average['df_type'], best_average['value']])
        return best_averages

    process_files(file_name, process_entry, finalize_results, ['Metric', 'Engine', 'DF_Type', 'Average Value'], 'best_averages.csv')

if __name__ == '__main__':
    calc_averages('all_engines_2011.01_2011.06')
    calc_best_averages('all_engines_2011.01_2011.06')
    #calc_averages('big_data_2011.01_2012.12')
    #calc_best_averages('big_data_2011.01_2012.12')