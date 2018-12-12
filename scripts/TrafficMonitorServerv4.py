"""
Server script to grab real time traffic data from AMAP API, save data to csv files and publish in realtime to thingspeak
"""
from datetime import datetime
import time, sys, pytz, requests, urllib2, traceback, os

apiKey1 = '...'  # AMAP API Key 1
apiKey2 = '...'  # AMAP API Key 2
thingSpeakAPI = '...'  # Thingspeak API Key
tsBase = 'https://api.thingspeak.com/update?api_key=%s' % thingSpeakAPI
cst = pytz.timezone('Asia/Shanghai')
logFormat = '%Y-%m-%d %H:%M:%S'
csvFormat = '%Y,%m,%d,%H,%M'
logFileFormat = '%Y%m%d_%H%M'
zhonglouX = '117.286013'
zhonglouY = '31.852868'
zhonglouRad = '500'
xiyouX = '117.233452'
xiyouY = '31.798127'
xiyouRad = '500'
zhanqianX1 = '117.309886'
zhanqianY1 = '31.880423'
zhanqianX2 = '117.318882'
zhanqianY2 = '31.886868'
ningguoX = '117.291309'
ningguoY = '31.847991'
ningguoRad = '500'


def processTrafficRadius(dataFile, x, y, rad, thingspeakField, apiKey):
    try:
        req = 'http://restapi.amap.com/v3/traffic/status/circle?location=' + x + ',' + y + '&radius=' + rad + '&key=' + apiKey
        resp = requests.get(req)
        if (resp.json()['status'] == '1'):
            # Parsing Response
            exp = resp.json()['trafficinfo']['evaluation']['expedite']
            cong = resp.json()['trafficinfo']['evaluation']['congested']
            blok = resp.json()['trafficinfo']['evaluation']['blocked']
            unk = resp.json()['trafficinfo']['evaluation']['unknown']
            dataFile.write(csvTime() + ',' + exp + ',' + cong + ',' + blok + ',' + unk + '\n')  # Write to file
            f = urllib2.urlopen(tsBase + '&' + thingspeakField + '=' + cong[:-1])  # Post to thingspeak
            time.sleep(15)
        else:
            logPrint('[ERROR] ' + logTime() + ' Response Status 0 for ' + dataFile.name)
    except Exception, e:
        logPrint('[ERROR] ' + logTime() + ' Program Raised Error:')
        logPrint(traceback.format_exc())


def processTrafficRectangle(dataFile, xx, yx, xy, yy, thingspeakField, apiKey):
    try:
        req = 'http://restapi.amap.com/v3/traffic/status/rectangle?rectangle=' + xx + ',' + yx + ';' + xy + ',' + yy + '&key=' + apiKey
        resp = requests.get(req)
        if (resp.json()['status'] == '1'):
            # Parsing Response
            exp = resp.json()['trafficinfo']['evaluation']['expedite']
            cong = resp.json()['trafficinfo']['evaluation']['congested']
            blok = resp.json()['trafficinfo']['evaluation']['blocked']
            unk = resp.json()['trafficinfo']['evaluation']['unknown']
            dataFile.write(csvTime() + ',' + exp + ',' + cong + ',' + blok + ',' + unk + '\n')  # Write to file
            f = urllib2.urlopen(tsBase + '&' + thingspeakField + '=' + cong[:-1])  # Post to thingspeak
            time.sleep(15)
        else:
            logPrint('[ERROR] ' + logTime() + ' Response Status 0 for ' + dataFile.name)
    except Exception, e:
        logPrint('[ERROR] ' + logTime() + ' Program Raised Error:')
        logPrint(traceback.format_exc())


def csvTime():
    return datetime.now(cst).strftime(csvFormat)


def logTime():
    return datetime.now(cst).strftime(logFormat)


def logPrint(content):
    print(content)


def main():
    logPrint('[LOG] ' + logTime() + ' Program Started')
    while True:
        try:
            zhonglou = open(os.path.join('/root/traffic/data', 'zhonglou.csv'), 'a')
            xiyou = open(os.path.join('/root/traffic/data', 'xiyou.csv'), 'a')
            zhanqian = open(os.path.join('/root/traffic/data', 'zhanqian.csv'), 'a')
            ningguo = open(os.path.join('/root/traffic/data', 'ningguo.csv'), 'a')
            processTrafficRadius(zhonglou, zhonglouX, zhonglouY, zhonglouRad, 'field1', apiKey1)
            time.sleep(15)
            processTrafficRadius(xiyou, xiyouX, xiyouY, xiyouRad, 'field2', apiKey1)
            time.sleep(15)
            processTrafficRectangle(zhanqian, zhanqianX1, zhanqianY1, zhanqianX2, zhanqianY2, 'field3', apiKey2)
            time.sleep(15)
            processTrafficRadius(ningguo, ningguoX, ningguoY, ningguoRad, 'field4', apiKey2)
            time.sleep(15)
            zhonglou.close()
            xiyou.close()
            zhanqian.close()
            ningguo.close()
        except KeyboardInterrupt:
            print 'Closing Files'
            zhonglou.close()
            xiyou.close()
            zhanqian.close()
            ningguo.close()
            print 'Server Gracefully Closed'
            sys.exit()


main()
