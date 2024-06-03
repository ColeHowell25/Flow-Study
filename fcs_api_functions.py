import requests
import xml.etree.ElementTree as ET
import datetime as dt
from ADS_API_functions import raise_server_error


#converts xml string into a python dictionary, the message field occurs twice in the tree so the len 9 is the outer shell
def xml_to_dict(xml_string):
    root = ET.fromstring(xml_string)
    result = {}
    for child in root:
        if len(child) == 0:
            result[child.tag] = child.text
        else:
            if 'message' not in result:
                result['message'] = []

            if child.tag == 'message' and len(child) == 9:
                result[child.tag].append(xml_to_dict(ET.tostring(child)))
            else:
                result[child.tag] = xml_to_dict(ET.tostring(child))
    return result


#gets the data from the fcs data gate service for the zone meter, returns a dictionary
def get_meter_data(sms, date):
    params = {
        'action': 'getmessages',
        'username': 'chowell',
        'password': 'Water1234!',
        'number': sms,
        'format': 'xml',
        'beginDate': str(date.strftime('%d-%m-%y')),
        'endDate': str(date.strftime('%d-%m-%y'))
    }

    response = requests.get('https://www.omnicoll.net/api/messagingapi.ashx', params)
    string = response.content.decode('utf-8')

    xml_dict = xml_to_dict(string)

    return xml_dict


#makes a new dictionary with only today's data
def today_data(xml_dict):
    today_dict = xml_dict
    # print(len(today_dict['message']))
    today_dict['message'] = [m for m in today_dict['message']
                             if dt.datetime.strptime(m['dateReceived'], '%Y-%m-%d %H:%M:%S.%f').date() == dt.datetime.today().date()]
    return today_dict


#there's something wrong with the data export api
def data_export(sms):
    params = {
        'export': 'txt',
        'username': 'chowell',
        'password': 'Water1234!',
        'software': 'DataGate2Admin',
        'logger': sms,
        'period': 0
    }

    response = requests.get('https://www.omnicoll.net/datagate/api/DataExportAPI.ashx', params)
    raise_server_error(response.status_code)
    data = response.content

    print(data.decode('utf-8'))


#attempt to use their streaming api, something is wrong with it
def stream():

    params = {
        'action': 'getmessages',
        'username': 'chowell',
        'password': 'Water1234!',
        'software': 'DataGate2Admin',
        'AccountName': 'WADC',
        'format': 'xml',
    }

    s = requests.Session()
    r = s.get('https://www.omnicoll.net/api/accountapi.ashx', headers=params, stream=True)
    print(r.status_code)
    for line in r.iter_lines():
        if line:
            print(line.decode('utf-8'))