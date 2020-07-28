import csv

with open('dataeng_test/dataset_short.csv', mode="r") as csv_file:
    csv_reader = csv.reader(csv_file)
    all_lines = []
    for row in csv_reader:
        all_lines.append(row)
    
    print(all_lines)