#Author: Cole Howell
#Purpose: Implementation of High Tide's api for data analysis purposes
#Update 10/5/2023: Changed get2AM to always acquire data based on passing 5AM utc for the current date into flow_calc

import datetime as dt
import requests
import json
import ADS_API_functions as ads
from datetime import date
import os
import calendar
import shutil
import openpyxl as xl
from arcgis import GIS
import copy
import time


#gets the possible api links to get the data for water from high tide, necessary for nrw calculations
def get_io_points_water(h):
    while True:
        response = requests.get(h['io_points_water'])
        if response.status_code < 400:
            raw_json = json.loads(response.content)
            break
    return raw_json


#gets all the possible api links that can be used to get the data for wastewater from high tide
#just prints them to console in a readable way, might have it return the string later
def get_io_points_sewer(h):
    response = requests.get(h['io_points_sewer'])
    ads.raise_server_error(response.status_code)
    raw_json = json.loads(response.content)
    pretty_json = json.dumps(raw_json, indent=4, separators=(',', ': '))

    return pretty_json


#get the stats data for yesterday from the sumiden pump station, returns a list of dictionaries for the pumps
def get_sumiden_stats(d, h):
    params = {
        "token": h['sewer_token'],
        "date": d #date
    }
    #parshall flume 1 flow
    response_1 = requests.get("https://api.htt.io/v2/customer/278/counter/3012/data", params)
    ads.raise_server_error(response_1.status_code)

    # parshall flume 2 flow
    response_2 = requests.get("https://api.htt.io/v2/customer/278/counter/3013/data", params)
    ads.raise_server_error(response_2.status_code)

    # discharge meter flow
    response_discharge = requests.get("https://api.htt.io/v2/customer/278/counter/3145/data", params)
    ads.raise_server_error(response_discharge.status_code)

    raw_1 = json.loads(response_1.content)
    raw_2 = json.loads(response_2.content)
    raw_3 = json.loads(response_discharge.content)

    return [raw_1, raw_2, raw_3]


#accepts a dictionary of pump runtimes by the hour as an argument and returns the total runtimes
def calc_total_runtime(pumps):
    runtimes = []
    for pump in pumps:
        runtime = 0
        for data in pump['data']:
            runtime += data['runtime']
        runtimes.append(runtime)

    runtimes = [i/(60**2) for i in runtimes]
    return runtimes


#I need to include these for part of the analysis for the flow study, it's easier to just make another dictionary
def study_dict_addition():
    dictionary = {
        "13 Fairview Park": [],
        "14 Horn Tavern Road": []
    }

    return dict(dictionary)



#dictionary of flow study zone meters
def flow_study_dict():
    dictionary = {
        "M01 County Line Meter": [],
        "P10 Harpeth Valley BPS": [],
        "06 Little John Lane": [],
        "10 Old Nashville Rd": [],
        "11 Lake Rd": [],
        "12 Northwest Hwy": []
    }

    return dict(dictionary)


#accepts an api link, token, and datetime as an argument, returns a dictionary containing the flow data for zone meter
def zone_meter_data(link, token, day):
    params = {
        "token": token,
        "date": day
    }
    while True:
        response = requests.get(link, params)
        if response.status_code < 400:
            # load json content into a dictionary
            data = json.loads(response.content)
            break

    return data


#parse the data dictionary for 2AM data
def get_twoAM(data):
    #if there is no data then the total should be recorded as zero
    if not data['data']:
        total = 0
    else:
        #there's some kind of offset on the api response when getting these numbers, this is for 2 AM though
        # the offset is the difference between UTC time and CST, I'm passing in the exact hour to start with now
        if len(data['data']) < 3:
            total = 0
        else:
            total = data['data'][2]['reading'] - data['data'][1]['reading']

    #this is the average gpm of the flow during the hour. just a unit conversion from gph -> gpm
    return total/60.0


#dictionary containing all the zone meters that I'm using to calculate nrw, values are empty lists to be filled later
def zone_meter_dict():
    zone_meters = {
        "M01 County Line Meter": [],
        "P10 Harpeth Valley BPS": [],
        "01 Old Franklin Rd": [],
        "02 Crow Cut Rd": [],
        "03 Fairview Blvd": [],
        "04 Cumberland Dr": [],
        "05 Glenhaven Dr": [],
        "06 Little John Lane": [],
        "07 Fairview Blvd": [],
        "08 Dice Lampley Rd": [],
        "09 Chester Rd": [],
        "10 Old Nashville Rd": [],
        "11 Lake Rd": [],
        "12 Northwest Hwy": [],
        "13 Fairview Park": [],
        "14 Horn Tavern Road": [],
        "France Tank": [],
        "Clearview Tank": [],
        "Sleepy Hollow Tank": []
    }

    return zone_meters


#store all the appropriate flow values in lists in a dictionary to be used to calculate nrw, syntax is
# [forward, reverse] for most with exception of clearview tank which is [BF, AR, AF, BR], accepts date and a template
# dictionary as arguments
def flow_calc(t, dict, h):

    #I'm not sure if either of these operations are absolutely necessary but it works
    zone_meters = dict
    allKeys = zone_meters.keys()
    ################################

    #required parameters for api query
    token = h['water_token']
    today = t

    #get all the io point links for wadc water
    scada_info = get_io_points_water(h)
    for data in scada_info['data']:
        #check to see if the site name is in the zone meter dictionary
        if data['name'] in allKeys:
            # loop through included counters
            for counter in data['counters']:
                #ignore these counters
                if counter['name'] == "Counter 1" or counter['name'] == "Counter 2" or \
                 counter['name'] == "Counter 3" or counter['name'] == "Counter 4":
                    continue
                else:
                    # print(counter['data-link'])
                    # append zone meter data to the dictionary, calling the api endpoint access function
                    zone_meters[data['name']].append(zone_meter_data(counter['data-link'], token, today))
        #if it isn't ignore it
        else:
            continue

    return zone_meters


#delete the extra counters in the zone meter data
def delete_extra(dictionary):
    #initialize start as 1 so the while loop works
    start = 1
    while start == 1:
        #loop over the outside keys in the dictionary
        for n in dictionary:
            # loop over the list stored in this key, also getting the index by using enumerate
            for idx, l in enumerate(dictionary[n]):
                # 'name' is the name of the volume counter stored in the list element
                if l['name'] not in ['Forward Total', 'Reverse Total', 'Total Flow']:
                    # if it's not one of the ones in the list above delete it and start the loop over again
                    del dictionary[n][idx]
                    start = 1
                    break
                # if all the list elements in the dictionary have name as one of the above then this condition lets
                # the loop end
                else:
                    start = 0
            if start == 1:
                break
    return dictionary


#take the raw readings from high tide and get the hourly flow
def extract_flow(t1, t2, dictionary, h):

    #deep copy makes a whole new dictionary instead of the variable simply referencing the original dictionary
    #this was the fix to a bug I found that was causing the data to not be stored in gis correctly
    d1 = copy.deepcopy(dictionary)
    d2 = copy.deepcopy(dictionary)

    #t1 gives the majority of the data I need, t2 gives the last hour necessary to do a full 24 hr flow calculation
    #delete extra deletes extra data counters in the dictionary from flow calc
    data1 = delete_extra(flow_calc(t1, d1, h))
    data2 = delete_extra(flow_calc(t2, d2, h))

    #loop over the data1 dictionary
    for n in data1:
        #loop over the list element in the nth key
        for idx, l in enumerate(data1[n]):
            #append the last data point from data2 to data 1 in the correct list
            if len(data2[n][idx]['data']) <= 1:
                continue

            l['data'].append(data2[n][idx]['data'][len(data2[n][idx]['data'])-1])
            #loop over the datapoint dictionary stored in the list, changing the datetime to a timestamp
            for d in range(0, len(l['data']) - 1):
                t_stamp = dt.datetime.timestamp(dt.datetime.strptime(l['data'][d]['datetime'].split('.', 1)[0], '%Y-%m-%dT%H:%M:%S'))

                #also necessary to find the change in readings to get the hourly flow
                l['data'][d]['flow'] = l['data'][d+1]['reading'] - l['data'][d]['reading']

                #there is an offset necessary to store the timestamp in gis correctly.
                #the offset is the difference in UTC time and central standard time which comes out to 5 hours or
                #18,000 seconds. This timestamp also needs to be converted to milliseconds to work in gis.

                #also note that daylight savings time adjusts the time difference between central time and utc time
                #so in daylight savings time the difference is 5 hours and when in standard time 6 hours
                if time.localtime().tm_isdst:
                    l['data'][d]['datetime'] = (t_stamp - 18000)*10**(3)
                else:
                    # print(t_stamp)
                    l['data'][d]['datetime'] = (t_stamp - 21600) * 10 ** (3)

    return data1


#send the data from the above function into the gis
def report_in_gis(data1, cutoff, g):

    #access the zone meter data table on gis
    gis = GIS("https://esriapps1.esriwadc.com/portal", g['username'], g['password'])
    meter_layer = gis.content.get('bb3d357ba01546fdac294b060aec7de0')
    data_table = meter_layer.tables[0]
    cutoff = dt.datetime.timestamp(cutoff) * 10 ** 3

    #loop through the keys in the data dictionary
    for n in data1:
        # there's a list of forward and reverse flow for most of the meters, but some only have total
        # if there are 2 elements do this
        if len(data1[n]) == 2:
            # loop through both forward and reverse data at the same time
            for i1, i2 in zip(data1[n][0]['data'], data1[n][1]['data']):
                #if flow is not in the data then ignore it
                if 'flow' not in i1.keys() or 'flow' not in i2.keys():
                    continue

                if i1['datetime'] <= cutoff:
                    # print('here')
                    continue

                # forward and reverse are flipped for northwest hwy in the high tide system
                if n == '12 Northwest Hwy':
                    add = {'attributes':
                        {
                            'meter_id': n[:3].rstrip(),
                            'date': i1['datetime'],
                            'forward_flow': i2['flow'],
                            'reverse_flow': i1['flow']
                        }

                    }
                else:
                    add = {'attributes':
                        {
                            'meter_id': n[:3].rstrip(),
                            'date': i1['datetime'],
                            'forward_flow': i1['flow'],
                            'reverse_flow': i2['flow']
                        }

                    }
                # print(add)
                data_table.edit_features(adds=[add])
        # this for if there's only one direction of flow in the meter, same process as above
        else:
            for i in data1[n][0]['data']:
                if 'flow' not in i.keys():
                    continue

                #if the date is less than or equal to the cutoff, skip the data point
                if i['datetime'] <= cutoff:
                    # print('here')
                    continue

                add = {'attributes':
                        {
                            'meter_id': n[:3].rstrip(),
                            'date': i['datetime'],
                            'forward_flow': i['flow'],
                            'reverse_flow': 0.0
                        }

                    }
                # print(add)
                data_table.edit_features(adds=[add])


#calculate the nrw for the zones, accepts a dictionary with flow information for the zone meters
def nrw_calc(z_met):
    user = os.environ['USERNAME']
    today = date.today()
    # directory where the yearly scans for water loss calculation for fairview are
    flow_dir = "C:/Users/" + user + "/WADC Dropbox/Cole Howell/PC/Documents/Flow Data/"

    # if the directory doesn't exist on the computer, make it
    if not os.path.exists(flow_dir):
        os.mkdir(flow_dir)

    # directory where the files will be moved to
    year_directory = flow_dir + str(today.year)
    month_directory = year_directory + "/" + calendar.month_name[today.month]
    store_directory = month_directory + "/" + str(today.day) + "/"

    # check whether the directory that the files are going to already exists
    if not os.path.exists(year_directory):
        os.mkdir(year_directory)
    if not os.path.exists(month_directory):
        os.mkdir(month_directory)
    if not os.path.exists(store_directory):
        os.mkdir(store_directory)

    # move a copy of the fairview analyzer to the directory
    flow_calculator = "ZoneMeters - Fairview Water Loss Calculator.xlsx"
    shutil.copy("C:/Users/" + user + "/WADC Dropbox/Cole Howell/PC/Documents/Flow Data/" + flow_calculator,
                store_directory + flow_calculator)

    wb = xl.load_workbook(store_directory + flow_calculator)
    sh = wb.active

    #have to do all this crap to enter these values into the excel sheet
    # put today's date into the sheet
    sh['A2'] = str(today) + " @ 2:00 AM (gpm)"

    # 01F
    sh['M15'] = z_met['01 Old Franklin Rd'][0]
    # 01R
    sh['M10'] = z_met['01 Old Franklin Rd'][1]
    # 02F
    sh['M16'] = z_met['02 Crow Cut Rd'][0]
    # 02R
    sh['M11'] = z_met['02 Crow Cut Rd'][1]
    # 03F
    sh['K14'] = z_met['03 Fairview Blvd'][0]
    # 03R
    sh['K11'] = z_met['03 Fairview Blvd'][1]
    # 04F
    sh['I9'] = z_met['04 Cumberland Dr'][0]
    # 04R
    sh['I13'] = z_met['04 Cumberland Dr'][1]
    # 05F
    sh['I10'] = z_met['05 Glenhaven Dr'][0]
    # 05R
    sh['I14'] = z_met['05 Glenhaven Dr'][1]
    # 06F
    sh['I11'] = z_met['06 Little John Lane'][0]
    # 06R
    sh['I15'] = z_met['06 Little John Lane'][1]
    # 07F
    sh['G22'] = z_met['07 Fairview Blvd'][0]
    # 07R
    sh['G14'] = z_met['07 Fairview Blvd'][1]
    # 08F
    sh['G23'] = z_met['08 Dice Lampley Rd'][0]
    # 08R
    sh['G15'] = z_met['08 Dice Lampley Rd'][1]
    # 09F
    sh['G24'] = z_met['09 Chester Rd'][1]
    # 09R
    sh['G16'] = z_met['09 Chester Rd'][0]
    # 10F
    sh['G10'] = z_met['10 Old Nashville Rd'][0]
    # 10R
    sh['G18'] = z_met['10 Old Nashville Rd'][1]
    # 11F
    sh['C15'] = z_met['11 Lake Rd'][0]
    # 11R
    sh['C10'] = z_met['11 Lake Rd'][1]
    # 12F
    sh['C16'] = z_met['12 Northwest Hwy'][1]
    # 12R
    sh['C11'] = z_met['12 Northwest Hwy'][0]
    # 13F
    sh['C17'] = z_met['13 Fairview Park'][0]
    # 13R
    sh['C12'] = z_met['13 Fairview Park'][1]
    # 14F
    sh['C18'] = z_met['14 Horn Tavern Road'][0]
    # 14R
    sh['C13'] = z_met['14 Horn Tavern Road'][1]
    # M01F
    sh['C9'] = z_met['M01 County Line Meter'][0]
    # M01R
    sh['C14'] = z_met['M01 County Line Meter'][1]
    # T15F
    sh['G9'] = z_met['France Tank'][0]
    # T15R
    sh['G17'] = z_met['France Tank'][1]
    # T16AF, up to change once all the meters are back in
    sh['G21'] = z_met['Clearview Tank'][1]
    # T16AR
    sh['G13'] = z_met['Clearview Tank'][2]
    # T16BF
    sh['E9'] = z_met['Clearview Tank'][0]
    # T16BR
    sh['E10'] = z_met['Clearview Tank'][3]
    # P10 HVUD Pump
    sh['S9'] = z_met['P10 Harpeth Valley BPS'][0]
    # T17BF
    sh['U9'] = z_met['Sleepy Hollow Tank'][1]
    # T17AF
    sh['M18'] = z_met['Sleepy Hollow Tank'][0]

    wb.save(store_directory + flow_calculator)

    #awful nrw list calculation derived from the locations of the meters in relation to their zones
    #couldn't think about how to loop this at the time, but probably could make a dictionary of all the sub-dmas
    #and then have a list of the meters associated inside the dictionary and loop over the list for each zone
    fv_1 = (z_met['M01 County Line Meter'][0] + z_met['11 Lake Rd'][1] + z_met['12 Northwest Hwy'][0] +
            z_met['13 Fairview Park'][1] + z_met['14 Horn Tavern Road'][1]) - \
           (z_met['M01 County Line Meter'][1] + z_met['11 Lake Rd'][0] + z_met['12 Northwest Hwy'][1] +
            z_met['13 Fairview Park'][0] + z_met['14 Horn Tavern Road'][0] + 28.7)

    cv_1 = (z_met['Clearview Tank'][0]) - (z_met['Clearview Tank'][3] + 3.3)

    f_1 = (z_met['11 Lake Rd'][0] + z_met['12 Northwest Hwy'][1] + z_met['France Tank'][0] +
           z_met['10 Old Nashville Rd'][0] + z_met['Clearview Tank'][2] + z_met['07 Fairview Blvd'][1] +
           z_met['08 Dice Lampley Rd'][1] + z_met['09 Chester Rd'][0]) - \
          (z_met['11 Lake Rd'][1] + z_met['12 Northwest Hwy'][0] + z_met['France Tank'][1] +
           z_met['10 Old Nashville Rd'][1] + z_met['Clearview Tank'][1] + z_met['07 Fairview Blvd'][0] +
           z_met['08 Dice Lampley Rd'][0] + z_met['09 Chester Rd'][1] + 32.3)

    f_2 = (z_met['04 Cumberland Dr'][0] + z_met['05 Glenhaven Dr'][0] + z_met['06 Little John Lane'][0] +
           z_met['09 Chester Rd'][1]) - (z_met['04 Cumberland Dr'][1] + z_met['05 Glenhaven Dr'][1] +
                                         z_met['06 Little John Lane'][1] + z_met['09 Chester Rd'][0] + 34.0)

    f_3 = (z_met['07 Fairview Blvd'][0] + z_met['05 Glenhaven Dr'][1] + z_met['03 Fairview Blvd'][1]) - \
          (z_met['07 Fairview Blvd'][1] + z_met['05 Glenhaven Dr'][0] + z_met['03 Fairview Blvd'][0] + 6.1)

    f_4 = (z_met['03 Fairview Blvd'][0] + z_met['01 Old Franklin Rd'][1] + z_met['02 Crow Cut Rd'][1] +
           z_met['04 Cumberland Dr'][1]) - (z_met['03 Fairview Blvd'][1] + z_met['01 Old Franklin Rd'][0] +
                                            z_met['02 Crow Cut Rd'][0] + z_met['04 Cumberland Dr'][0] +
                                            z_met['Sleepy Hollow Tank'][0] + 11.2)

    f_5 = (z_met['01 Old Franklin Rd'][0]) - (z_met['01 Old Franklin Rd'][1] + 8.0)

    f_6 = (z_met['02 Crow Cut Rd'][0] + z_met['08 Dice Lampley Rd'][0]) - (z_met['02 Crow Cut Rd'][1] +
                                                                           z_met['08 Dice Lampley Rd'][1] + 6.8)

    hvud_1 = (z_met['P10 Harpeth Valley BPS'][0] + z_met['10 Old Nashville Rd'][1] + z_met['06 Little John Lane'][1] +
              z_met['13 Fairview Park'][0] + z_met['14 Horn Tavern Road'][0]) - \
             (z_met['10 Old Nashville Rd'][0] + z_met['06 Little John Lane'][0] +
              z_met['13 Fairview Park'][1] + z_met['14 Horn Tavern Road'][1] + 37.2)

    sh_1 = (z_met['Sleepy Hollow Tank'][1]) - 6.1

    nrw_list = [fv_1, cv_1, f_1, f_2, f_3, f_4, f_5, f_6, hvud_1, sh_1]

    return nrw_list


#nrw calculations without sending data to excel sheets first
def nrw_calc_new(z_met):
    # awful nrw list calculation derived from the locations of the meters in relation to their zones
    fv_1 = (z_met['M01 County Line Meter'][0] + z_met['11 Lake Rd'][1] + z_met['12 Northwest Hwy'][0] +
            z_met['13 Fairview Park'][1] + z_met['14 Horn Tavern Road'][1]) - \
           (z_met['M01 County Line Meter'][1] + z_met['11 Lake Rd'][0] + z_met['12 Northwest Hwy'][1] +
            z_met['13 Fairview Park'][0] + z_met['14 Horn Tavern Road'][0] + 28.7)

    cv_1 = (z_met['Clearview Tank'][0]) - (z_met['Clearview Tank'][3] + 3.3)

    f_1 = (z_met['11 Lake Rd'][0] + z_met['12 Northwest Hwy'][1] + z_met['France Tank'][0] +
           z_met['10 Old Nashville Rd'][0] + z_met['Clearview Tank'][2] + z_met['07 Fairview Blvd'][1] +
           z_met['08 Dice Lampley Rd'][1] + z_met['09 Chester Rd'][0]) - \
          (z_met['11 Lake Rd'][1] + z_met['12 Northwest Hwy'][0] + z_met['France Tank'][1] +
           z_met['10 Old Nashville Rd'][1] + z_met['Clearview Tank'][1] + z_met['07 Fairview Blvd'][0] +
           z_met['08 Dice Lampley Rd'][0] + z_met['09 Chester Rd'][1] + 32.3)

    f_2 = (z_met['04 Cumberland Dr'][0] + z_met['05 Glenhaven Dr'][0] + z_met['06 Little John Lane'][0] +
           z_met['09 Chester Rd'][1]) - (z_met['04 Cumberland Dr'][1] + z_met['05 Glenhaven Dr'][1] +
                                         z_met['06 Little John Lane'][1] + z_met['09 Chester Rd'][0] + 34.0)

    f_3 = (z_met['07 Fairview Blvd'][0] + z_met['05 Glenhaven Dr'][1] + z_met['03 Fairview Blvd'][1]) - \
          (z_met['07 Fairview Blvd'][1] + z_met['05 Glenhaven Dr'][0] + z_met['03 Fairview Blvd'][0] + 6.1)

    f_4 = (z_met['03 Fairview Blvd'][0] + z_met['01 Old Franklin Rd'][1] + z_met['02 Crow Cut Rd'][1] +
           z_met['04 Cumberland Dr'][1]) - (z_met['03 Fairview Blvd'][1] + z_met['01 Old Franklin Rd'][0] +
                                            z_met['02 Crow Cut Rd'][0] + z_met['04 Cumberland Dr'][0] +
                                            z_met['Sleepy Hollow Tank'][1] + 11.2)

    f_5 = (z_met['01 Old Franklin Rd'][0]) - (z_met['01 Old Franklin Rd'][1] + 8.0)

    f_6 = (z_met['02 Crow Cut Rd'][0] + z_met['08 Dice Lampley Rd'][0]) - (z_met['02 Crow Cut Rd'][0] +
                                                                           z_met['08 Dice Lampley Rd'][0] + 6.8)

    hvud_1 = (z_met['P10 Harpeth Valley BPS'][0] + z_met['10 Old Nashville Rd'][1] + z_met['06 Little John Lane'][1] +
              z_met['13 Fairview Park'][0] + z_met['14 Horn Tavern Road'][0]) - \
             (z_met['10 Old Nashville Rd'][0] + z_met['06 Little John Lane'][0] +
              z_met['13 Fairview Park'][1] + z_met['14 Horn Tavern Road'][1] + 37.2)

    sh_1 = (z_met['Sleepy Hollow Tank'][0]) - 6.1

    nrw_list = [fv_1, cv_1, f_1, f_2, f_3, f_4, f_5, f_6, hvud_1, sh_1]

    return nrw_list


#gets the level of the France tank for specified date from high tide, returns a dictionary of recorded heights
def france_tank_level(date, h):
    params = {
        'token': h['water_token'],
        'date': date
    }

    response = requests.get("https://api.htt.io/v2/customer/15/analog/391/data", params)
    height = json.loads(response.content)

    return height
