#Author: Cole Howell
#functions used in the fairview flow study

from arcgis import GIS
from datetime import datetime
import datetime as dt
import high_tide_api_functions as htt
import beacon_api_functions as bapi
import os
import traceback
import calendar
from arcgis.geometry import filters
import configparser


#configure api functions with information from a file
def config():
    config = configparser.ConfigParser()
    config.read('C:\EsriTraining\PythonGP\Scripts\wadc_dev_config.ini')

    #file contains username, password, and api token/parameter information
    gis = config['GIS']
    bcon = config['beacon']
    htt = config['high_tide']

    return gis, bcon, htt


def visualize(h):
    gis = GIS("https://esriapps1.esriwadc.com/portal", h['username'], h['password'])
    zone_item = gis.content.get('cbbfdb6383a34030bf667d1cc9614a33')
    zone_layer = zone_item.layers[0]
    zone_fset = zone_layer.query(where="zonemtr = 'Hwy_96' OR zonemtr = 'HVUD_1' OR zonemtr = 'FV_1'")
    features = zone_fset.features

    # gets the utc timestamp of today's date
    date = dt.datetime.today().replace(hour=4, minute=0, second=0)

    #get the nrw log and query for today's date
    table = zone_item.tables[0]
    table_set = table.query(where=f"date_recorded >= TIMESTAMP \'{date}\'")
    rows = table_set.features

    #initialize variables
    total_nrw = {
        'Hwy_96': 0,
        'HVUD_1': 0,
        'FV_1': 0
    }
    total_perc_nrw = {
        'Hwy_96': 0,
        'HVUD_1': 0,
        'FV_1': 0
    }

    count = {
        'Hwy_96': 0,
        'HVUD_1': 0,
        'FV_1': 0
    }

    #loop through the rows in the table to get the low flow hours data
    for r in rows:
        if r.attributes['date_recorded'] >= dt.datetime.timestamp(date.replace(hour=4, minute=0, second=0)) * 10 ** 3:
            continue
        else:
            total_nrw[r.attributes['zone']] += r.attributes['corrected_nrw']
            total_perc_nrw[r.attributes['zone']] += r.attributes['percent_nrw']
            count[r.attributes['zone']] += 1

    perc_nrw = {}
    #get the mean percent nrw
    perc_nrw['Hwy_96'] = total_perc_nrw['Hwy_96'] / count['Hwy_96']
    perc_nrw['HVUD_1'] = total_perc_nrw['HVUD_1'] / count['HVUD_1']
    perc_nrw['FV_1'] = total_perc_nrw['FV_1'] / count['FV_1']

    for f in features:
        f.attributes['non_revenue_water'] = total_nrw[f.attributes['zonemtr']]/(5*60)
        f.attributes['percent_nrw'] = perc_nrw[f.attributes['zonemtr']]

        zone_layer.edit_features(updates=[f])


def badger_collection(b, g):
    # collect data for the badger meters
    badger = bapi.collect_all(b)
    # for b in badger:
        # print(len(badger[b]))

    bapi.store_in_gis(badger, g)


#collect data from high tide
def collect_high_tide(dictionary, h, g):

    if calendar.day_name[dt.datetime.today().weekday()] != 'Monday':
        # t1 and t2 gives all the data I need for the high tide meters
        t1 = (dt.datetime.now() - dt.timedelta(days=1)).replace(hour=10, minute=0, second=0)
        t2 = (dt.datetime.now() - dt.timedelta(days=1)).replace(hour=11, minute=0, second=0)

        # t1 = dt.datetime(2023, 12, 28, 10, 0, 0)
        # t2 = dt.datetime(2023, 12, 28, 11, 0, 0)
        # collect the high tide data
        high_tide = htt.extract_flow(t1, t2, dictionary, h)
        cutoff = t1.replace(hour=3, minute=0, second=0)
        htt.report_in_gis(high_tide, cutoff, g)
        # print(high_tide)
        s_date = datetime.timestamp(
            datetime.now().replace(hour=4, minute=0, second=0) - dt.timedelta(days=1)) * 10 ** (3)
        # s_date = dt.datetime(2023, 12, 28, 9, 0, 0)
    else:
        for t in range(0, 3):
            t1 = (dt.datetime.now() - dt.timedelta(days=(3 - t))).replace(hour=10, minute=0, second=0)
            t2 = (dt.datetime.now() - dt.timedelta(days=(3 - t))).replace(hour=11, minute=0, second=0)
            # print(t1)

            # t1 = (dt.datetime.now() - dt.timedelta(days=1)).replace(hour=10, minute=0, second=0)
            # t2 = (dt.datetime.now() - dt.timedelta(days=1)).replace(hour=11, minute=0, second=0)

            high_tide = htt.extract_flow(t1, t2, dictionary, h)
            # print(high_tide)
            cutoff = t1.replace(hour=4, minute=0, second=0)
            htt.report_in_gis(high_tide, cutoff, g)
        s_date = datetime.timestamp(
            datetime.now().replace(hour=4, minute=0, second=0) - dt.timedelta(days=3)) * 10 ** (3)

    return s_date


#original flow study data
def study_hwy_96(g, b, h):
    try:
        badger_collection(b, g)
        #collect all the data and return the start date
        s_date = collect_high_tide(htt.flow_study_dict(), h, g)

        #access the gis
        gis = GIS("https://esriapps1.esriwadc.com/portal", g['username'], g['password'])

        #get the rows of the zone meters' data table
        zone_meter_layer = gis.content.get('bb3d357ba01546fdac294b060aec7de0')
        z_meter_table = zone_meter_layer.tables[0]
        z_meter_set = z_meter_table.query()
        z_meter_rows = z_meter_set.features

        #get the badger meter data table rows
        badger_meter_layer = gis.content.get('62e76f6d62d543c0ad5c4954e2156efd')
        b_meter_feature = badger_meter_layer.layers[0]

        #query the badger layer for data only in the zone
        b_layer_fset = b_meter_feature.query(where="in_zone = '1'")
        b_meter_features = b_layer_fset.features

        #get the object ids for all the badger meters
        object_ids = [f.attributes['objectid'] for f in b_meter_features]

        #query the related records for the selected meters
        data_table = b_meter_feature.query_related_records(object_ids=','.join(map(str, object_ids)), relationship_id='0')

        b_meter_rows = data_table

        #get the hwy 96 data table rows
        hwy_96 = gis.content.get('cbbfdb6383a34030bf667d1cc9614a33')
        hwy_96_table = hwy_96.tables[0]

        #initialize an empty dictionary to use
        data = {}

        #loop through the queried zone meter data table, skipping dates that are already in the nrw table
        for r in z_meter_rows:

            #ignore data before the start date
            # if r.attributes['date'] < s_date or r.attributes['date'] >= e_date:
            #     continue

            if r.attributes['date'] < s_date:
                continue
            # print(r.attributes['date'])
            #if the date is not in the data dictionary keys then add it and initialize the keys within
            if r.attributes['date'] not in data.keys():
                # add these values to the data dict
                data[r.attributes['date']] = {'Pump Totals': 0, 'Zone Meter Totals': 0, 'Badger Totals': 0, 'Reverse': 0}

            #if the meter id for meters is either of the pumps then add their flow to pump totals
            if r.attributes['meter_id'] == 'M01' or r.attributes['meter_id'] == 'P10':
                data[r.attributes['date']]['Pump Totals'] += r.attributes['forward_flow']


            #otherwise add the difference of reverse flow and forward flow, forward flow represents flow out of zone
            else:
                data[r.attributes['date']]['Zone Meter Totals'] += (r.attributes['reverse_flow'] - r.attributes['forward_flow'])
                data[r.attributes['date']]['Reverse'] += r.attributes['reverse_flow']


        #loop through the queried related records, skipping dates that are none or less than the selected date adding data
        for q in b_meter_rows['relatedRecordGroups']:

            #related records have a bunch of extra keys to go through to get to the data so loop through all of that
            for b in q['relatedRecords']:

                if b['attributes']['flow_time'] is None or b['attributes']['flow_time'] < s_date:
                    continue
                try:
                    # add flow badger meter flow to dictionary, totaling for an hour across all meters in the zone
                    data[(b['attributes']['flow_time'])]['Badger Totals'] += b['attributes']['flow']
                except KeyError:
                    continue

        # print(data)
        #add the data dictionary to the gis
        for t in data:
            #gets the datetime object from the timestamp and converts it to central standard time
            time = dt.datetime.fromtimestamp(t*10**(-3), dt.timezone(dt.timedelta(hours=-5)))
            corrected_nrw = data[t]['Pump Totals'] + data[t]['Zone Meter Totals'] - data[t]['Badger Totals'] - 1842
            perc_nrw = corrected_nrw/(data[t]['Pump Totals'] + data[t]['Reverse'])

            add = {'attributes':
                       {
                           'zone': 'Hwy_96',
                           'date_recorded': t,
                           'badger_totals': -1*data[t]['Badger Totals'],
                           'zone_meter_totals': data[t]['Zone Meter Totals'],
                           'z_reverse': data[t]['Reverse'],
                           'pump_totals': data[t]['Pump Totals'],
                           'non_revenue_water': data[t]['Pump Totals'] + data[t]['Zone Meter Totals'] - data[t]['Badger Totals'],
                           'corrected_nrw': corrected_nrw,
                           'percent_nrw': perc_nrw,
                           'hour': time.hour
                       }
                   }
            hwy_96_table.edit_features(adds=[add])

        visualize(g)

    except Exception:
        today = dt.datetime.today()
        filename = 'fairview_flow_study_errors.txt'
        path = f'C:/Users/chowell/WADC Dropbox/Cole Howell/PC/Documents/Flow Data/Hwy 96 Reports/{filename}'
        if os.path.exists(path):
            f = open(path, 'a')
            f.write(f'{today}\n')
            traceback.print_exc(file=f)
            f.write(f'\n')
        else:
            f = open(path, 'w')
            f.write(f'{today}\n')
            traceback.print_exc(file=f)
            f.write(f'\n')


#perform the zone study for FV_1 and HVUD_1 zones separated
def separate_zone_study(g, b, h):
    #collect the data from the badger meters
    badger_collection(b, g)
    #performs collection and gives the start date that is used to filter data
    f_study = htt.flow_study_dict()
    f_study.update(htt.study_dict_addition())
    s_date = collect_high_tide(f_study, h, g)
    # s_date = datetime.timestamp(
    #     datetime.now().replace(hour=4, minute=0, second=0) - dt.timedelta(days=1)) * 10 ** (3)
    # s_date = datetime.timestamp(dt.datetime(2024, 2, 13, 10, 0, 0))*10**3
    # print(s_date)
    # e_date = datetime.timestamp(dt.datetime(2024, 5, 6, 5, 0, 0))*10**3
    # s_date = collect_high_tide(htt.study_dict_addition())
    # print(s_date)

    # s_date = datetime.timestamp(
    #     datetime.now().replace(hour=22, minute=0, second=0) - dt.timedelta(days=5)) * 10 ** (3)

    # access the gis
    gis = GIS("https://esriapps1.esriwadc.com/portal", g['username'], g['password'])

    # get the Fairview flow study layer
    flow_study = gis.content.get('cbbfdb6383a34030bf667d1cc9614a33')
    flow_study_lyr = flow_study.layers[0]
    flow_study_tbl = flow_study.tables[0]

    study_fset = flow_study_lyr.query(where="zonemtr = 'HVUD_1' OR zonemtr = 'FV_1' or zonemtr = 'Hwy_96'")
    # study_fset = flow_study_lyr.query(where="zonemtr = 'HVUD_1' OR zonemtr = 'FV_1'")
    study_features = study_fset.features
    # print(len(study_features))

    # get the badger meter layer
    badger_meter_layer = gis.content.get('62e76f6d62d543c0ad5c4954e2156efd')
    b_meter_lyr = badger_meter_layer.layers[0]

    #get the badger meter data table
    b_table = badger_meter_layer.tables[0]

    # get the rows of the zone meters' layer
    zone_meter_layer = gis.content.get('bb3d357ba01546fdac294b060aec7de0')
    z_meter_lyr = zone_meter_layer.layers[0]

    #unavoidable real losses for each zone in gal/h
    uarl = {
        'Hwy_96': 1842,
        'FV_1': 846,
        'HVUD_1': 996
    }

    # print(study_features)
    #loop through the DMA boundaries to filter the other layers and perform calculations
    for s in study_features:
        # print(s)
        sr = s.geometry['spatialReference']
        zm_filter = filters.touches(s.geometry, sr)
        badger_filter = filters.contains(s.geometry, sr)

        #perform a spatial filter on the zone meters where they must touch the DMA boundary
        z_fset = z_meter_lyr.query(geometry_filter=zm_filter)
        z_features = z_fset.features
        # print(z_features)

        # query related records of the badger meters within the filtered zone
        b_fset = b_meter_lyr.query(geometry_filter=badger_filter)
        b_features = b_fset.features
        # print(len(b_features))
        # continue

        #get the object ids from the selected zone meters and then query the related records for the appropriate data
        z_id = [f.attributes['objectid'] for f in z_features]
        # print(len(z_id))
        z_data = z_meter_lyr.query_related_records(object_ids=','.join(map(str, z_id)), relationship_id='0', definition_expression=f"date > '{dt.datetime.fromtimestamp(s_date*10**(-3))}'")
        # print(z_data)

        #do the same with the badger meters
        # b_id = [f.attributes['objectid'] for f in b_features]
        #query the badger table directly
        b_sn = [f"'{f.attributes['user_meter_sn']}'" for f in b_features]

        tab_set = b_table.query(where=f"endpoint_sn IN({','.join(map(str, b_sn))}) AND flow_time >= '{dt.datetime.fromtimestamp(s_date*10**(-3))}'")
        b_rows = tab_set.features
        # print(tab_rows)
        # continue


        # print(len(b_id))
        # print(b_id)
        # b_data = b_meter_lyr.query_related_records(object_ids=','.join(map(str, b_id)), relationship_id='0',definition_expression=f"flow_time > '{dt.datetime.fromtimestamp(s_date*10**(-3))}'")
        # with open('C:/Users/chowell/WADC Dropbox/Cole Howell/PC/Documents/Flow Data/badger_records.txt', 'w') as f:
        #     f.write(json.dumps(b_data, indent=4))
        # continue
        # this is the format of related records structure:
        # z_data['relatedRecordGroups'][list of grouped meters]['relatedRecords'][list of rows]

        z_list = z_data['relatedRecordGroups']
        # b_list = b_data['relatedRecordGroups']
        # print(len(z_list))
        # print(len(b_list))
        # continue
        data = {}
        check = 0
        # print(z_list[0]['relatedRecords'][len(z_list[0]['relatedRecords'])-1])
        #loop through the zone meter data list
        for z_group in z_list:
            # print(len(z_group['relatedRecords']))
            for z in z_group['relatedRecords']:
                # print(z)
                # set date key variable to the entries timestamp
                d_key = z['attributes']['date']
                if d_key > check:
                    check = d_key

                # print(d_key)
                #if the date key is greater than or equal to the start date do this stuff
                if d_key >= s_date and d_key:
                    # print(z)
                    #initialize the date key entry in the data dictionary if it is not already there
                    if d_key not in data.keys():
                        data[d_key] = {'Pump': 0, 'Z_tot': 0, 'B_tot': 0, 'z_rvs': 0}
                    # assign data to the correct locations
                    if z['attributes']['meter_id'] == 'M01' or z['attributes']['meter_id'] == 'P10':
                        data[d_key]['Pump'] += z['attributes']['forward_flow']
                        #reverse flow for M01 is out of the zone, I'm accounting for it in this way
                        if z['attributes']['meter_id'] == 'M01':
                            data[d_key]['Z_tot'] -= z['attributes']['reverse_flow']
                            data[d_key]['z_rvs'] -= z['attributes']['reverse_flow']
                    else:
                        #for Horn Tavern Rd and Fairview Park forward flow is into HVUD_1 and reverse is out
                        if s.attributes['zonemtr'] == 'HVUD_1' and (z['attributes']['meter_id'] == '13' or z['attributes']['meter_id'] == '14'):
                            data[d_key]['Z_tot'] += z['attributes']['forward_flow'] - z['attributes']['reverse_flow']
                            data[d_key]['z_rvs'] += z['attributes']['forward_flow']
                        else:
                            data[d_key]['Z_tot'] += z['attributes']['reverse_flow'] - z['attributes']['forward_flow']
                            data[d_key]['z_rvs'] += z['attributes']['reverse_flow']
        for b in b_rows:
            d_key = b.attributes['flow_time']
            if d_key is not None and d_key >= s_date:
                try:
                    data[d_key]['B_tot'] += b.attributes['flow']
                    # print(str(d_key) + "\t" + str(data[d_key]['B_tot']))
                except KeyError:
                    continue

        # continue
         # print(check)
        # loop through the badger meter data list
        # for b_group in b_list:
        #     for b in b_group['relatedRecords']:
        #         date key
                # d_key = b['attributes']['flow_time']
                # do this if d_key has a value and is greater than or equal to the start date
                # if d_key is not None and d_key >= s_date:
                #     try:
                #         data[d_key]['B_tot'] += b['attributes']['flow']
                #     except KeyError:
                #         continue
        # print(data)
        for t in data:
            # gets the datetime object from the timestamp and converts it to central standard time
            time = dt.datetime.fromtimestamp(t * 10 ** (-3), dt.timezone(dt.timedelta(hours=-5)))
            corrected_nrw = data[t]['Pump'] + data[t]['Z_tot'] - data[t]['B_tot'] - uarl[s.attributes['zonemtr']]
            if data[t]['Pump'] == data[t]['z_rvs'] == 0:
                perc_nrw = None
            else:
                perc_nrw = corrected_nrw / (data[t]['Pump'] + data[t]['z_rvs'])

            add = {'attributes':
                {
                    'zone': s.attributes['zonemtr'],
                    'date_recorded': t,
                    'badger_totals': -1 * data[t]['B_tot'],
                    'zone_meter_totals': data[t]['Z_tot'],
                    'z_reverse': data[t]['z_rvs'],
                    'pump_totals': data[t]['Pump'],
                    'non_revenue_water': data[t]['Pump'] + data[t]['Z_tot'] - data[t]['B_tot'],
                    'corrected_nrw': corrected_nrw,
                    'percent_nrw': perc_nrw,
                    'hour': time.hour
                }
            }
            # print(add)
            flow_study_tbl.edit_features(adds=[add])

