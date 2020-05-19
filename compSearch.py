import MySQLdb
import requests
from tqdm import tqdm
import datetime
import time
import json


def get_cookie():
    with open('cookie.txt', 'r') as f:
        ck = f.readlines()
    return ck[0]


def request_with_retry(url: str, param: dict = '', hdr: dict = ''):
    """
    Allow requests.get to re-try three times

    :param url: str: URL to request
    :param param: dict: Query parameters
    :param hdr: dict: Request headers
    :return: resp: requests.models.Response: Response of the request
    """

    # Create a flag for failure
    flag = True

    # Try three times
    for i in range(3):
        try:
            resp = requests.post(url, data=param, headers=hdr, timeout=120)
            return resp
        except:
            time.sleep(5)

    # if failed, return failed
    if flag:
        return 'failed'


def pull(hdr):
    url = 'http://edp.resset.com/adt/company/searchcompany'

    # Read all locations
    with open('allLocation.txt', 'r', encoding='gbk') as f:
        locations = f.readlines()

    # Loop over dates
    dates = date_range('1972-01-01', '1973-01-01')
    for date in dates:
        # Loop over all locations
        for location in tqdm(locations, desc='Registered on ' + date, mininterval=300):
            location = location.strip('\n').split('\t')
            for status in range(1, 11):
                for capital in [0, 4, 5, 6, 7, 8, 9]:
                    # Post data
                    form = {'pageNo': '1',
                            'pageSize': '100',
                            'regCapitalFlag': str(capital),
                            'regStatusFlag': str(status),
                            'divsionSearch': ''.join(location),
                            'esstarttime': date,
                            'esendtime': date}
                    resp = request_with_retry(url, hdr=hdr, param=form)
                    # Save data
                    data = json.loads(resp.text)
                    if data['message'] == 'success':
                        if len(data['data']['totalStr']) >= 3:
                            print(location + ' ' + date + ' Failed')
                            with open('failed.csv', 'a+') as f:
                                f.writelines(''.join(location) + ',' + date + '\n')
                        else:
                            data = data['data']['companys']
                            for company in data:
                                company['companyID'] = company.pop('id')
                                company['establishTime'] = company.pop('estiblishTime')
                                del company['companyId']
                                for key in company:
                                    if company[key] == '-':
                                        company[key] = ''
                                    company[key] = str(company[key])
                                company['date'] = datetime.datetime.utcnow().date().isoformat()
                                company['prov'] = location[0]
                                company['pref'] = location[1]
                                # Save to MySQL
                                sql = 'insert into firm (%s) ' \
                                      'values ("%s") ' \
                                      'on duplicate key update' \
                                      '    date = "%s"'\
                                      % (', '.join(company.keys()),
                                         '", "'.join(company.values()),
                                         company['date'])
                                cur.execute(sql)
                            con.commit()
                    else:
                        print(location + ' ' + date + ' Failed')
                        with open('failed.csv', 'a+') as f:
                            f.writelines(''.join(location) + ',' + date + '\n')


def date_range(begin_date, end_date):
    """
    Get all dates between begin date and end date (left-closed and right-open)

    :param begin_date: str: Begin date, e.g. 2013-01-01
    :param end_date: str: End date, e.g. 2013-05-01
    :return: List: All dates (in str) between begin and end date
    """
    dates = []
    dt = datetime.datetime.strptime(begin_date, "%Y-%m-%d")
    date = begin_date[:]
    while date < end_date:
        dates.append(date)
        dt = dt + datetime.timedelta(1)
        date = dt.strftime("%Y-%m-%d")
    return dates


if __name__ == '__main__':
    # Connect to SQL server
    con = MySQLdb.connect(host='wos.c1atmg3oa5jd.us-east-1.rds.amazonaws.com',
                          user='root', passwd='IynVnIk(M4w9', charset='utf8')
    cur = con.cursor()
    cur.execute('use firm;')

    # Get cookies
    cookie = get_cookie()
    header = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                      'AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/84.0.4136.5 Safari/537.36',
        'Cookie': cookie,
        'Host': 'edp.resset.com',
        'Origin': 'http://edp.resset.com',
        'Referer': 'http://edp.resset.com/adt/enuser/index?wefvbh=1&key=adt%2Cspssdsm%2Cwbdkey=2'
    }

    # Download info
    pull(header)
