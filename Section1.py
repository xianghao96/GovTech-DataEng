import csv

with open('dataeng_test/dataset_short.csv', mode="r") as csv_file:
    csv_reader = csv.reader(csv_file)
    all_lines = []
    single_line = []

    for row in csv_reader:
        # Remove rows with no name
        if len(row[0]) == 0:
            pass

        else:
            # Remove prepended 0s
            row[1] = row[1].strip("0")
            
            if row[0] != "name":
                if float(row[1]) > 100:
                    row.append("true")
                else:
                    row.append("false")

            # Split the names to first and last names
            split_row = ' '.join(row).split()

            if len(split_row) == 4:
                # Remove Mr. and Ms.
                split_row.pop(0)

            all_lines.append(split_row)

    # Create new columns for new csv
    all_lines[0] = ["first_name", "last_name", "price", "above_100"]    
    
    print(all_lines)