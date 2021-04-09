from datetime import datetime
import time, sys, pytz, traceback, requests, os, logging, schedule
from func_timeout import func_set_timeout
from urllib.request import urlopen
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from api_key import *
tsBase = 'https://api.thingspeak.com/update?api_key=%s' % thingSpeakAPI
cst = pytz.timezone('Asia/Shanghai')
csvFormat = '%Y,%m,%d,%H,%M'
zhonglou = {'x': '117.286013', 'y': '31.852868', 'rad': '500', 'file': 'zhonglou.csv'}
xiyou = {'x': '117.233452', 'y': '31.798127', 'rad': '500', 'file': 'xiyou.csv'}
zhanqian = {'x1': '117.309886', 'x2': '117.318882', 'y1': '31.880423', 'y2': '31.886868', 'file': 'zhanqian.csv'}
ningguo = {'x': '117.291309', 'y': '31.847991', 'rad': '500', 'file': 'ningguo.csv'}
ts_buffer = {'data': ['', '', '', ''], 'new': [False, False, False, False]}
# create logger
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
# create file handler which logs only error
fh = logging.FileHandler('traffic_error.log')
fh.setLevel(logging.ERROR)
# create console handler with a higher log level
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# fh.setFormatter(formatter)
ch.setFormatter(formatter)
# add the handlers to the logger
# logger.addHandler(fh)
logger.addHandler(ch)
# Setup requests
request_session = requests.Session()
retries = Retry(total=5, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504])
request_session.mount('http://', HTTPAdapter(max_retries=retries))

# Initialize api key generator
def get_amap_api():
    """Yield (loop) through API keys in list api_keys"""
    assert(isinstance(api_keys, list))
    current_api_index = 0
    n_keys = len(api_keys)
    while True:
        current_api_index = (current_api_index + 1) % n_keys
        yield api_keys[current_api_index]

amap_key = get_amap_api()

@func_set_timeout(10)
def process_traffic_radius(region, ts_field_id, api_key):
    try:
        req = 'http://restapi.amap.com/v3/traffic/status/circle?location=' + region['x'] + ',' + region['y']\
              + '&radius=' + region['rad'] + '&key=' + api_key
        logger.debug('Calling AMAP: ' + req)
        # resp = requests.get(req)
        resp = http_request(req)
        logger.debug(resp.json())
        if resp.json()['status'] == '1':
            # Parsing Response
            exp = resp.json()['trafficinfo']['evaluation']['expedite']
            cong = resp.json()['trafficinfo']['evaluation']['congested']
            blok = resp.json()['trafficinfo']['evaluation']['blocked']
            unk = resp.json()['trafficinfo']['evaluation']['unknown']
            with open(os.getcwd() + '/data/' + region['file'], 'a') as data_file:
                data_file.write(csv_time() + ',' + exp + ',' + cong + ',' + blok + ',' + unk + '\n')  # Write to file
            ts_buffer['data'][ts_field_id-1] = cong[:-1]
            ts_buffer['new'][ts_field_id-1] = True
        else:
            logger.error('Response Status 0 for ' + region['file'])
    except:
        logger.error(traceback.format_exc())

@func_set_timeout(10)
def process_traffic_rectangle(region, ts_field_id, api_key):
    try:
        req = 'http://restapi.amap.com/v3/traffic/status/rectangle?rectangle=' + region['x1'] + ','\
              + region['y1'] + ';' + region['x2'] + ',' + region['y2'] + '&key=' + api_key
        logger.debug('Calling AMAP: ' + req)
        # resp = requests.get(req)
        resp = http_request(req)
        logger.debug(resp.json())
        if resp.json()['status'] == '1':
            # Parsing Response
            exp = resp.json()['trafficinfo']['evaluation']['expedite']
            cong = resp.json()['trafficinfo']['evaluation']['congested']
            blok = resp.json()['trafficinfo']['evaluation']['blocked']
            unk = resp.json()['trafficinfo']['evaluation']['unknown']
            with open(os.getcwd() + '/data/' + region['file'], 'a') as data_file:
                data_file.write(csv_time() + ',' + exp + ',' + cong + ',' + blok + ',' + unk + '\n')  # Write to file
            ts_buffer['data'][ts_field_id-1] = cong[:-1]
            ts_buffer['new'][ts_field_id-1] = True
        else:
            logger.error('Response Status 0 for ' + region['file'])
    except:
        logger.error(traceback.format_exc())

@func_set_timeout(10)
def post_thingspeak():
    try:
        if all(ts_buffer['new']):
            ts_url = tsBase + '&' + 'field1' + '=' + ts_buffer['data'][0] \
                  + '&' + 'field2' + '=' + ts_buffer['data'][1] \
                  + '&' + 'field3' + '=' + ts_buffer['data'][2] \
                  + '&' + 'field4' + '=' + ts_buffer['data'][3]
            logger.debug('Posting to ThingSpeak: ' + ts_url)
            urlopen(ts_url)
            ts_buffer['new'] = [False, False, False, False]
    except:
        logger.error(traceback.format_exc())

def http_request(link, response=request_session):
    try:
        return response.get(link, timeout=5)
    except:
        logger.error(traceback.format_exc())

def csv_time():
    return datetime.now(cst).strftime(csvFormat)

def process_zhonglou(): 
    try:
        process_traffic_radius(zhonglou, 1, next(amap_key))
    except:
        # NOTE: Must end this process gracefully so last_run on schedule could be properly updated
        logger.error(traceback.format_exc())  # This should only happen for func_timeout for above function

def process_xiyou():
    try:
        process_traffic_radius(xiyou, 2, next(amap_key))
    except:
        # NOTE: Must end this process gracefully so last_run on schedule could be properly updated
        logger.error(traceback.format_exc())  # This should only happen for func_timeout for above function

def process_zhanqian():
    try:
        process_traffic_rectangle(zhanqian, 3, next(amap_key))
    except:
        # NOTE: Must end this process gracefully so last_run on schedule could be properly updated
        logger.error(traceback.format_exc())  # This should only happen for func_timeout for above function

def process_ningguo():
    try:
        process_traffic_radius(ningguo, 4, next(amap_key))
    except:
        # NOTE: Must end this process gracefully so last_run on schedule could be properly updated
        logger.error(traceback.format_exc())  # This should only happen for func_timeout for above function

def process_thingspeak():
    try:
        post_thingspeak()
    except:
        # NOTE: Must end this process gracefully so last_run on schedule could be properly updated
        logger.error(traceback.format_exc())  # This should only happen for func_timeout for above function


if __name__ == '__main__':
    logger.info('Program initializing')
    schedule.every().minute.do(process_zhonglou)
    schedule.every().minute.do(process_xiyou)
    schedule.every().minute.do(process_zhanqian)
    schedule.every().minute.do(process_ningguo)
    schedule.every(30).seconds.do(process_thingspeak)
    logger.info('Program started')
    while True:
        try:
            schedule.run_pending()
            time.sleep(1)
        except KeyboardInterrupt:
            logger.info('Server manually closed by KeyboardInterrupt')
            sys.exit()
        except:
            # Should never reach here because all tasks are try-catched
            logger.error("THIS SHOULD NOT PRINT")
            logger.error(traceback.format_exc())