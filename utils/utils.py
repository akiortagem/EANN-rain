import csv
import os
import urllib2
import numpy as np
import time

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
    try:
        return float(temperature) if temperature else -9999
    except:
        return -9999

def row_get_dewpoint(row):
    dew_point = row[2]
    try:
        return float(dew_point) if dew_point else -9999
    except:
        return -9999
    
def row_get_humidity(row):
    humidity = row[3]
    try:
        return float(humidity) if humidity else -9999
    except:
        return -9999

def row_get_slp(row):
    slp = row[4]
    try:
        return float(slp) if slp else -9999
    except:
        return -9999

def row_get_windspeed(row):
    windspeed = row[7]
    if windspeed and windspeed != 'Calm':
        windspeed = float(windspeed)
    elif windspeed == 'Calm':
        windspeed = 0.0
    else:
        windspeed = -9999
    return windspeed

def row_get_rain(row):
    return 1 if 'Rain' in row[10] else 0

def row_distance_to_djf(timestamp):
    december = datetime(timestamp.year, 12, 1)
    february = datetime(timestamp.year, 2, 28)
    delta = abs(timestamp - december).days if abs(timestamp - december).days < abs(timestamp - february).days else abs(timestamp - february).days
    return delta if not timestamp.month in [12,1,2] else 0

def row_distance_to_jja(timestamp):
    june = datetime(timestamp.year, 6, 1)
    august = datetime(timestamp.year, 8, 31)
    delta = abs(timestamp - june).days if abs(timestamp - june).days < abs(timestamp - august).days else abs(timestamp - august).days
    return delta if not timestamp.month in [6,7,8] else 0
    

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
        raw_dict[index]['djf'] = row_distance_to_djf(index)
        raw_dict[index]['jja'] = row_distance_to_jja(index)
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
            if tstamp_complete and not -9999 in data_row:
                data_row.extend([
                        raw_dict[tstamp_inc]['djf'],
                        raw_dict[tstamp_inc]['jja']
                ])
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
    
def download_wunderground_history(icao, date):
    url = 'http://www.wunderground.com/history/airport/' + icao
    url += '/' + str(date.year) + '/' + str(date.month) + '/' + str(date.day)
    url += '/DailyHistory.html?format=1'
    print 'downloading from %s' % url
    response = urllib2.urlopen(url)
    content = response.read()
    if 'TimeWIB' in content:
        content = content.replace('<br />', '')
        content = content.replace('\n', '',1)
        data_dir = 'Data/' + icao + '/'
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)
        csv_file = data_dir + date.strftime('%Y%m%d') + '.csv'
        csv_file = open(csv_file, "w")
        csv_file.write(content)
        csv_file.close
    else:
        print 'something wrong with %s file error' % url 

def bulk_download_wu_history(icao, start_date, end_date):
    while start_date <= end_date:
        download_wunderground_history(icao, start_date)
        start_date = start_date + timedelta(days=1)
        time.sleep(10)
    

        
    