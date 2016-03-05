import csv

def load_file(file):
    with open(file, 'rb') as f:
        reader = csv.reader(f)
        result = list(reader)
    return result