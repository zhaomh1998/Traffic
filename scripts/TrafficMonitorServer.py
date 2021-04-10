from datetime import datetime, timedelta
import time, sys, pytz, traceback, requests, os, logging, croniter
from func_timeout import func_set_timeout
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from api_key import *

min_per_sample = 10
tsBase = 'https://api.thingspeak.com/update?api_key=%s' % thingSpeakAPI
cst = pytz.timezone('Asia/Shanghai')
csvFormat = '%Y,%m,%d,%H,%M'
zhonglou = {'center': '31.858673,117.292678', 'rad': '500', 'coord_type': 'bd09ll',
            'roads': ['徽州大道', '芜湖路', '芜湖路辅路'], 'file': 'zhonglou.csv'}
xiyou = {'center': '31.804602,117.239061', 'rad': '500', 'coord_type': 'bd09ll',
         'roads': ['习友路', '习友路辅路', '习友西路', '潜山路', '潜山路辅路'], 'file': 'xiyou.csv'}
zhanqian = {'center': '31.889066,117.321993', 'rad': '500', 'coord_type': 'bd09ll',
            'roads': ['站前路', '胜利路', '胜利路辅路'], 'file': 'zhanqian.csv'}
ningguo = {'center': '31.853656,117.297822', 'rad': '500', 'coord_type': 'bd09ll',
           'roads': ['宁国南路', '宁国路', '南一环路', '南一环路辅路'], 'file': 'ningguo.csv'}

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
retries = Retry(total=5, backoff_factor=5, status_forcelist=[500, 502, 503, 504])
request_session.mount('http://', HTTPAdapter(max_retries=retries))

# Set up data directory
data_folder = os.path.join(os.getcwd(), 'data')
if not os.path.exists(data_folder):
    os.mkdir(data_folder)


# Initialize api key generator
def get_baidu_api():
    """Yield (loop) through API keys in list api_keys"""
    assert(isinstance(api_keys_baidu, list))
    current_api_index = 0
    n_keys = len(api_keys_baidu)
    while True:
        current_api_index = (current_api_index + 1) % n_keys
        yield api_keys_baidu[current_api_index]


map_api_key = get_baidu_api()


@func_set_timeout(10)
def process_traffic_radius(region, ts_field_id, api_key):
    try:
        req = 'http://api.map.baidu.com/traffic/v1/around?ak=' + api_key + '&center=' + region['center']\
              + '&radius=' + region['rad'] + '&coord_type_input=' + region['coord_type']\
              + '&coord_type_input=' + region['coord_type']

        logger.debug('Calling API: ' + req)
        resp = http_request(req)
        resp_parsed = resp.json()
        logger.debug(resp_parsed)
        if resp_parsed['status'] == 0:
            # Parsing Response
            road_traffic = resp_parsed['road_traffic']
            assert isinstance(road_traffic, list)
            assert len(road_traffic) != 0
            road_traffic_parsed = {
                i['road_name']: i['congestion_sections'] if 'congestion_sections' in i.keys() else None
                for i in road_traffic
            }
            road_traffic_need = {k: road_traffic_parsed[k] for k in region['roads']}
            log_path = os.path.join(data_folder, region['file'])
            if not os.path.exists(log_path):
                with open(log_path, 'w') as data_file:
                    data_file.write('Year,Month,Day,Hour,Minute,Road,Status,Desc,Distance,Speed,Trend\n')

            total_cong_distance = 0
            with open(log_path, 'a') as data_file:
                time_str = csv_time()
                data_file.write(time_str + ',' + '区域,' + str(resp_parsed['evaluation']['status']) + ','
                                + str(resp_parsed['description'].replace(',', '，')) + ',-1,-1,NA\n')
                for k, v in road_traffic_need.items():
                    if v is not None:
                        assert isinstance(v, list)
                        assert len(v) > 0
                        for section in v:
                            data_file.write(
                                f'{time_str},{k}'
                                + ',' + str(section['status'])
                                # To avoid confusion with csv delimiter, we replace the comma in description
                                + ',' + str(section['section_desc'].replace(',', '，'))
                                + ',' + str(section['congestion_distance'])
                                + ',' + str(section['speed'])
                                + ',' + str(section['congestion_trend']) + '\n'
                            )
                            total_cong_distance += section['congestion_distance']

            ts_buffer['data'][ts_field_id-1] = str(total_cong_distance)
            ts_buffer['new'][ts_field_id-1] = True
        else:
            logger.error('API Call Error for ' + region['file'] + ': ' + resp.json()['message'])
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
            http_request(ts_url)
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
        process_traffic_radius(zhonglou, 1, next(map_api_key))
    except:
        # NOTE: Must end this process gracefully so last_run on schedule could be properly updated
        logger.error(traceback.format_exc())  # This should only happen for func_timeout for above function

def process_xiyou():
    try:
        process_traffic_radius(xiyou, 2, next(map_api_key))
    except:
        # NOTE: Must end this process gracefully so last_run on schedule could be properly updated
        logger.error(traceback.format_exc())  # This should only happen for func_timeout for above function

def process_zhanqian():
    try:
        process_traffic_radius(zhanqian, 3, next(map_api_key))
    except:
        # NOTE: Must end this process gracefully so last_run on schedule could be properly updated
        logger.error(traceback.format_exc())  # This should only happen for func_timeout for above function

def process_ningguo():
    try:
        process_traffic_radius(ningguo, 4, next(map_api_key))
    except:
        # NOTE: Must end this process gracefully so last_run on schedule could be properly updated
        logger.error(traceback.format_exc())  # This should only happen for func_timeout for above function

def process_thingspeak():
    try:
        post_thingspeak()
    except:
        # NOTE: Must end this process gracefully so last_run on schedule could be properly updated
        logger.error(traceback.format_exc())  # This should only happen for func_timeout for above function


# Scheduler
# https://stackoverflow.com/a/35833199
# Round time down to the top of the previous minute
def round_down_time(dt=None, dateDelta=timedelta(minutes=1)):
    roundTo = dateDelta.total_seconds()
    if dt is None:
        dt = datetime.now()
    seconds = (dt - dt.min).seconds
    rounding = (seconds + roundTo / 2) // roundTo * roundTo
    return dt + timedelta(0, rounding - seconds, -dt.microsecond)


# Get next run time from now, based on schedule specified by cron string
def get_next_cron_runtime(schedule):
    return croniter.croniter(schedule, datetime.now()).get_next(datetime)


# Sleep till the top of the next minute
def sleep_until_next_minute():
    t = datetime.utcnow()
    time.sleep(60 - (t.second + t.microsecond / 1000000.0))


if __name__ == '__main__':
    logger.info('Program initializing')
    seconds_until_next_sample = (min_per_sample * 60) - time.time() % (min_per_sample * 60)
    logger.info('Will wait %.2f seconds before starting' % seconds_until_next_sample)
    time.sleep(seconds_until_next_sample - 1)
    logger.info('Program started')

    sched_every_x_min = f'*/{min_per_sample} * * * *'  # Run every x minutes
    t_next_sched = get_next_cron_runtime(sched_every_x_min)

    while True:
        try:
            t_rounded = round_down_time()
            if t_rounded == t_next_sched:
                process_zhonglou()
                logger.info('[OK] Zhonglou')
                process_xiyou()
                logger.info('[OK] Xiyou')
                process_zhanqian()
                logger.info('[OK] Zhanqian')
                process_ningguo()
                logger.info('[OK] Ningguo')
                process_thingspeak()
                logger.info('[OK] ThingSpeak\n')
                t_next_sched = get_next_cron_runtime(sched_every_x_min)
            elif t_rounded > t_next_sched:
                # We missed an execution
                logger.error("Execution Missed!")
                t_next_sched = get_next_cron_runtime(sched_every_x_min)
            sleep_until_next_minute()
            time.sleep(1)  # Delay 1 second to avoid edge issues
        except KeyboardInterrupt:
            logger.info('Server manually closed by KeyboardInterrupt')
            sys.exit()
        except:
            # Should never reach here because all tasks are try-catched
            logger.error("THIS SHOULD NOT PRINT")
            logger.error(traceback.format_exc())
