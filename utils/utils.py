import csv
import os
import numpy as np

from datetime import datetime, timedelta

def load_file(file, headers=True):
    with open(file, 'rb') as f:
        reader = csv.reader(f)
        result = list(reader)
        if not headers:
            del result[0]
    return result

def row_get_time(row):
    return datetime.strptime(row[-1], '%Y-%m-%d %H:%M:%S')

def row_get_temperature(row):
    temperature = row[1]
    return float(temperature) if temperature else float('nan')

def row_get_dewpoint(row):
    dew_point = row[2]
    return float(dew_point) if dew_point else float('nan')
    
def row_get_humidity(row):
    humidity = row[3]
    return float(humidity) if humidity else float('nan')

def row_get_slp(row):
    slp = row[4]
    return float(slp) if slp else float('nan')

def row_get_windspeed(row):
    windspeed = row[7]
    if windspeed and windspeed != 'Calm':
        windspeed = float(windspeed)
    elif windspeed == 'Calm':
        windspeed = 0.0
    else:
        windspeed = float('nan')
    return windspeed

def row_get_rain(row):
    return 1 if 'Rain' in row[10] else 0

#def row_distance_from_djf(row, djf_peak_month):
    

def get_dataset(file, lead_time=3):
    print 'processing %s' % file
    raw_dict={}
    raw = load_file(file, headers=False)
    timestamps = []
    for row in raw:
        index = row_get_time(row)
        raw_dict[index] = {}
        timestamps.append(row_get_time(row))
        raw_dict[index]['timestamp']=(row_get_time(row))
        raw_dict[index]['temperature']=(row_get_temperature(row))
        raw_dict[index]['dewpoint']=(row_get_dewpoint(row))
        raw_dict[index]['humidity']=(row_get_humidity(row))
        raw_dict[index]['slp']=(row_get_slp(row))
        raw_dict[index]['windspeed']=(row_get_windspeed(row))
        raw_dict[index]['rain']=(row_get_rain(row))
    dataset_x = []
    dataset_y = []
    for index, row in raw_dict.items():
        hr_lead = row['timestamp'] + timedelta(hours=(lead_time))
        hr_rain = hr_lead + timedelta(hours=1)
        tstamp_inc = index
        if hr_lead in timestamps and hr_rain in timestamps:
            data_row = []
            data_rain_row = []
            tstamp_complete = True
            while tstamp_inc <= hr_lead:
                if tstamp_inc in timestamps:
                    data_row.extend([
                        raw_dict[tstamp_inc]['temperature'],
                        raw_dict[tstamp_inc]['dewpoint'],
                        raw_dict[tstamp_inc]['humidity'],
                        raw_dict[tstamp_inc]['slp'],
                        raw_dict[tstamp_inc]['windspeed']
                    ])
                else:
                    print "%s not proceessed because %s (for X) not in timestamps" % (index.strftime('%Y-%m-%d %H:%M:%S'),hr_lead.strftime('%Y-%m-%d %H:%M:%S'))
                    data_row = []
                    tstamp_complete = False
                    break
                tstamp_inc = tstamp_inc + timedelta(hours=1)
            if tstamp_complete:
                rain_row = raw_dict[hr_rain]
                data_rain_row.extend([rain_row['rain']])
                dataset_x.append(data_row)
                dataset_y.append(data_rain_row)
        elif not hr_lead in timestamps:
            print "%s not proceessed because %s (for X) not in timestamps" % (index.strftime('%Y-%m-%d %H:%M:%S'),hr_lead.strftime('%Y-%m-%d %H:%M:%S'))
        elif not hr_rain in timestamps:
            print "%s not proceessed because %s (for y) not in timestamps" % (index.strftime('%Y-%m-%d %H:%M:%S'),hr_rain.strftime('%Y-%m-%d %H:%M:%S'))
            
    return (np.array(dataset_x), np.array(dataset_y))

def get_dataset_from_dir(path):
    file_list = os.listdir(path)
    csvs = []
    for filename in file_list:
        if '.csv' in filename:
            csvs.append(path + filename)
    X, y = get_dataset(csvs[-1])
    del csvs[-1]
    for csv_file in csvs:
        Xx, yy = get_dataset(csv_file)
        X = np.vstack([X, Xx])
        y = np.vstack([y,yy])
    return (X, y)

        
    