import csv
from nameparser import HumanName

with open('dataeng_test/dataset.csv', mode="r") as csv_file:
    csv_reader = csv.reader(csv_file)
    all_lines = []
    single_line = []

    for row in csv_reader:
        # Remove rows with no name
        if len(row[0]) == 0:
            pass

        else:
            # Remove any other than first and last names
            name = HumanName(row[0])
            name.string_format = "{first} {last}"
            row[0] = str(name)
            
            # Remove prepended 0s
            row[1] = row[1].strip("0")
            
            if row[0] != "name":
                if float(row[1]) > 100:
                    row.append("true")
                else:
                    row.append("false")

            # Split the names to first and last names
            split_row = ' '.join(row).split()

            all_lines.append(split_row)

    # Create new columns for new csv
    all_lines[0] = ["first_name", "last_name", "price", "above_100"]    

with open("parsed_dataset.csv", "w", newline="") as file:
     writer = csv.writer(file)
     writer.writerows(all_lines)