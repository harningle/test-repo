import MySQLdb
import requests
from requests.packages import urllib3
import copy
import time
import os
import re
from bs4 import BeautifulSoup
from tqdm import tqdm
import json
from datetime import datetime


def login():
    """
    This func. performs SAML authentication at WOS.

    :return: session: requests.sessions.Session: A successfully logged session
    """

    # Open a session
    session = requests.session()

    # Disable insecure request warning
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    # Open the page
    hdr = {'User-Agent': userAgent,
           'Connection': 'keep-alive',
           'Host': 'www.webofknowledge.com'}
    resp = session.get('https://www.webofknowledge.com', headers=hdr, allow_redirects=False)

    # Python requests does not support javascript, so it cannot automatically redirect to the
    # correct webpage. We need to manually post the form data and goto the correct login page.
    form_data = {'Error': 'IPError',
                 'PathInfo': '%2F',
                 'RouterURL': 'https%3A%2F%2Fwww.webofknowledge.com%2F',
                 'Domain': '.webofknowledge.com',
                 'Src': 'IP',
                 'Alias': 'WOK5'}
    hdr['Host'] = 'login.webofknowledge.com'
    resp = session.get(url='https://login.webofknowledge.com/error/Error', headers=hdr,
                       params=form_data)

    # Use CHINA CERNET Federation to login
    soup = BeautifulSoup(resp.text, features='lxml')
    shibboleth = soup.find_all('option')
    for i in shibboleth:
        if 'CHINA CERNET Federation' in i.text:
            shibboleth = i['value']
            break

    # Go to the login page of CHINA CERNET Federation
    hdr['Host'] = 'ds.carsi.edu.cn'
    resp = session.get(shibboleth, headers=hdr)

    # Again, bc. Python requests does not support javascript, we need to manually redirect
    resp = session.get(resp.url.replace('DS/Carsifed.wayf', 'ds/index.html'), headers=hdr)

    # Select Fudan
    hdr['Referer'] = resp.url
    hdr['Host'] = 'www.webofknowledge.com'
    resp = session.get(
        'https://ds.carsi.edu.cn/DS/pingback?selectIdP=1&IDPName=idpfudan.fudan.edu.cn',
        headers=hdr)
    param = {'auth': 'ShibbolethIdPForm',
             'target': 'https%3A%2F%2Fwww.webofknowledge.com%2F%3FIsProductCode%3DYes%26Error'
                       '%3DIPError'
                       '%26PathInfo%3D%252F%26RouterURL%3Dhttps%253A%252F%252Fwww.webofknowledge'
                       '.com%25'
                       '2F%26Domain%3D.webofknowledge.com%26Src%3DIP%26Alias%3DWOK5'
                       '%26ShibFederation%3D'
                       'ChineseFederation',
             'entityID': 'https://idpfudan.fudan.edu.cn/idp/shibboleth'}
    resp = session.get('https://www.webofknowledge.com/', headers=hdr, params=param)

    # Again and again, requests does not support javascript...
    soup = BeautifulSoup(resp.text, features='lxml')
    form_data = soup.find_all('input')
    form_data = {i['name']: i['value'] for i in form_data if i.has_attr('name')}
    del hdr['Referer']
    hdr['Host'] = 'idpfudan.fudan.edu.cn'
    resp = session.post('https://idpfudan.fudan.edu.cn/idp/profile/SAML2/POST/SSO',
                        headers=hdr,
                        data=form_data,
                        verify=False)

    # Login using Fudan UIS account
    form_data = {'j_username': '18210680076',
                 'j_password': 'Fudan1211',
                 '_eventId_proceed': ''}
    hdr['Referer'] = resp.url
    resp = session.post(resp.url, headers=hdr, data=form_data)

    # Javascript redirect...
    soup = BeautifulSoup(resp.text, features='lxml')
    form_data = soup.find_all('input')
    form_data = {i['name']: i['value'] for i in form_data if i.has_attr('name')}
    hdr['Host'] = 'www.webofknowledge.com'
    hdr['Origin'] = 'https://idpfudan.fudan.edu.cn'
    resp = session.post('https://www.webofknowledge.com/?auth=Shibboleth', headers=hdr,
                        data=form_data, allow_redirects=False)
    del hdr['Origin']
    resp = session.get(resp.headers['location'], headers=hdr, allow_redirects=False)
    hdr['Host'] = 'apps.webofknowledge.com'
    resp = session.get(resp.headers['location'], headers=hdr)

    # Check whether logged
    if 'Fudan' in resp.text:
        print('Logged')
        return session
    else:
        return 'Failed'


def request_with_retry(url: str, param: dict = '', hdr: dict = '', method: str = 'Get'):
    """
    Allow requests.get to re-try three times

    :param url: str: URL to request
    :param param: dict: Query parameters
    :param hdr: dict: Request headers
    :param method: str: Get (default) or Post
    :return: resp: requests.models.Response: Response of the request
    """

    # Create a flag for failure
    flag = True

    # Global s, or we cannot revise it
    global s

    # Try three times
    for i in range(3):
        # Copy session
        session = copy.deepcopy(s)
        try:
            if method == 'Post':
                # Post
                resp = session.post(url, data=param, headers=hdr, timeout=60)
            else:
                # Get
                resp = session.get(url, params=param, headers=hdr, timeout=60)
            # if succeeded, replace global s with session
            s = session
            return resp
        except:
            time.sleep(5)

    # if failed, return failed
    if flag:
        return 'failed'


def retrieve_article(issn: str, year: int):
    """
    Get all articles and their links published in this journal in this year

    :param issn: str: ISSN of the journal
    :param year: int: year
    """

    # URL for GeneralSearch
    url = 'https://apps.webofknowledge.com/WOS_AdvancedSearch.do'

    # Form data for post
    form_data = [('product', 'WOS'),
                 ('search_mode', 'AdvancedSearch'),
                 ('action', 'search'),
                 ('replaceSetId', ''),
                 ('goToPageLoc', 'SearchHistoryTableBanner'),
                 ('value(input1)', 'IS=(%s)' % issn),
                 ('value(searchOp)', 'search'),
                 ('value(select2)', 'LA'),
                 ('value(input2)', ''),
                 ('value(select3)', 'DT'),
                 ('value(input3)', ''),
                 ('value(limitCount)', '14'),
                 ('limitStatus', 'expanded'),
                 ('ss_lemmatization', 'On'),
                 ('ss_spellchecking', 'Suggest'),
                 ('SinceLastVisit_UTC', ''),
                 ('SinceLastVisit_DATE', ''),
                 ('range', 'CUSTOM'),
                 ('period', 'Year Range'),
                 ('startYear', str(year)),
                 ('endYear', str(year)),
                 ('editions', 'AHCI'),
                 ('editions', 'SCI'),
                 ('editions', 'SSCI'),
                 ('update_back2search_link_param', 'yes'),
                 ('ss_query_language', ''),
                 ('rs_sort_by', 'PY.D;LD.D;SO.A;VL.D;PG.A;AU.A')]

    # Search articles published in this journal and year
    resp = request_with_retry(url, param=form_data, hdr=header, method='Post')

    # Go to search result
    soup = BeautifulSoup(resp.text, features='lxml')
    url = soup.find_all('div', class_='historyResults')[0]

    # If no articles are found, it probably means that journal has not yet been created in this year
    # But that may also means there're something wrong with our search, so save the ISSN and year
    # into a .csv for further manual check.
    if url.text.strip() == '0':
        with open('emptySearch.csv', 'a+') as f:
            f.writelines(issn + ',' + str(year) + '\n')
        return

    # Open the page of result
    url = url.a['href']
    url = 'https://apps.webofknowledge.com' + url

    # Export article info.
    export_article(url, issn, year)
    pass


def export_article(url: str, issn: str, year: int):
    """
    Export articles info.

    :param url: str: URL of the search result page
    :param issn: str: ISSN (or eISSN) of the journal
    :param year: int: Publication year of these articles
    """

    # Open the search result page
    resp = request_with_retry(url, hdr=header)

    # See how many articles are there in this search
    soup = BeautifulSoup(resp.text, features='lxml')
    num_of_article = soup.find_all('span', id='trueFinalResultCount')[0]
    num_of_article = int(num_of_article.text.strip().replace(',', ''))

    # rurl is required for downloading article info.
    rurl = soup.find_all('input', id='rurl')[0]
    rurl = rurl['value']

    # qid of this search
    qid = re.findall(r'qid=(\d+)', url)[0]

    # We use the export func. in WOS to collect the info. of articles in this search. Below is the
    # URL, header, and form data necessary for export
    hdr = copy.deepcopy(header)
    hdr['Referer'] = url
    form_data = {'selectedIds': '',
                 'displayCitedRefs': 'true',
                 'displayTimesCited': 'true',
                 'displayUsageInfo': 'true',
                 'viewType': 'fullRecord',
                 'product': 'WOS',
                 'rurl': rurl,
                 'mark_id': 'WOS',
                 'colName': 'WOS',
                 'search_mode': 'AdvancedSearch',
                 'view_name': 'WOS-fullRecord',
                 'sortBy': 'PY.D;LD.D;SO.A;VL.D;PG.A;AU.A',
                 'mode': 'OpenOutputService',
                 'qid': qid,
                 'format': 'saveToFile',
                 'filters': 'HIGHLY_CITED HOT_PAPER OPEN_ACCESS PMID USAGEIND '
                            'AUTHORSIDENTIFIERS '
                            'ACCESSION_NUM FUNDING SUBJECT_CATEGORY JCR_CATEGORY LANG IDS '
                            'PAGEC '
                            'SABBR CITREFC ISSN PUBINFO KEYWORDS CITTIMES ADDRS '
                            'CONFERENCE_SPONSORS DOCTYPE CITREF ABSTRACT CONFERENCE_INFO '
                            'SOURCE '
                            'TITLE AUTHORS  ',
                 'queryNatural': 'IS=(%s)' % issn,
                 'count_new_items_marked': '0',
                 'use_two_ets': 'false',
                 'IncitesEntitled': 'no',
                 'value(record_select_type)': 'range',
                 'fields_selection': 'HIGHLY_CITED HOT_PAPER OPEN_ACCESS PMID USAGEIND '
                                     'AUTHORSIDENTIFIERS ACCESSION_NUM FUNDING '
                                     'SUBJECT_CATEGORY '
                                     'JCR_CATEGORY LANG IDS PAGEC SABBR CITREFC ISSN PUBINFO '
                                     'KEYWORDS CITTIMES ADDRS CONFERENCE_SPONSORS DOCTYPE '
                                     'CITREF '
                                     'ABSTRACT CONFERENCE_INFO SOURCE TITLE AUTHORS  ',
                 'save_options': 'tabWinUTF8'}
    url = 'https://apps.webofknowledge.com/OutboundService.do?action=go&&'

    # Bc. there is a limit that we can only export up to 500 articles at a single time, so we need
    # to check whether the total #. of articles in this search (num_of_article) exceeds this limit.
    # If not, we export from the first article to the num_of_article-th article
    if num_of_article < 500:
        # Set the no. of the first and last article
        form_data['mark_to'] = str(num_of_article)
        form_data['mark_from'] = '1'
        form_data['markFrom'] = '1'
        form_data['markTo'] = str(num_of_article)
        # Export info.
        resp = request_with_retry(url, param=form_data, hdr=hdr, method='Post')
        # Sometimes WOS will export null. Save these cases into a .csv file
        if resp.text == '\ufeffnull':
            with open('exportFailed.csv', 'a+') as f:
                f.writelines('%s,%d' % (issn, year))
            return
        # Clean the exported info.
        info = resp.content.decode('UTF-8-sig')
        if info.startswith('null'):
            with open('exportFailed.csv', 'a+') as f:
                f.writelines('%s,%d' % (issn, year))
            return
        info = info.rstrip('\t\r\n').replace('\t\r\n', '\r\n').split('\r\n')
        info = [i.split('\t') for i in info]
        # The var. name IS should be changes, since "is" is a logic operator in MySQL
        info[0][info[0].index('IS')] = 'ISS'
        # Save into MySQL
        for i in info[1:]:
            # Escape special characters
            for j in range(len(i)):
                i[j] = i[j].replace('\\', r'\\')
                i[j] = i[j].replace('"', r'\"')
            sql = 'insert into article (%s) values ("%s") ' \
                  'on duplicate key update' \
                  '    UT = "%s"'\
                  % (', '.join(info[0]), '", "'.join(i), i[info[0].index('UT')])
            cur.execute(sql)
        con.commit()
    else:
        # Download 500 articles a time
        for no in range(1, num_of_article, 500):
            start = no
            if no + 499 > num_of_article:
                end = num_of_article
            else:
                end = no + 499
            # Set the no. of the first and last article in this download
            form_data['mark_to'] = str(num_of_article)
            form_data['mark_from'] = '1'
            form_data['markFrom'] = '1'
            form_data['markTo'] = str(num_of_article)
            # The below codes are merely the same as the case when #. of articles is less than 500
            resp = request_with_retry(url, param=form_data, hdr=hdr, method='Post')
            # Sometimes WOS will export null. Save these cases into a .csv file
            if resp.text == '\ufeffnull':
                with open('exportFailed.csv', 'a+') as f:
                    f.writelines('%s,%d' % (issn, year))
                return
            # Clean the exported info.
            info = resp.content.decode('UTF-8-sig')
            if info.startswith('null'):
                with open('exportFailed.csv', 'a+') as f:
                    f.writelines('%s,%d' % (issn, year))
                return
            info = resp.content.decode('UTF-8-sig')
            info = info.rstrip('\t\r\n').replace('\t\r\n', '\r\n').split('\r\n')
            info = [i.split('\t') for i in info]
            info[0][info[0].index('IS')] = 'ISS'
            for i in info[1:]:
                for j in range(len(i)):
                    i[j] = i[j].replace('\\', r'\\')
                    i[j] = i[j].replace('"', r'\"')
                sql = 'insert into article (%s) values ("%s") ' \
                      'on duplicate key update' \
                      '    UT = "%s"' \
                      % (', '.join(info[0]), '", "'.join(i), i[info[0].index('UT')])
                cur.execute(sql)
            con.commit()
    pass


if __name__ == '__main__':
    # Connect to SQL server
    con = MySQLdb.connect(host='wos.c1atmg3oa5jd.us-east-1.rds.amazonaws.com',
                          user='root', passwd='IynVnIk(M4w9', charset='utf8')
    cur = con.cursor()
    cur.execute('use WOS;')

    # Set user agent
    userAgent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)' \
                'AppleWebKit/537.36 (KHTML, like Gecko)' \
                'Chrome/83.0.4103.14 Safari/537.36'
    header = {'User-Agent': userAgent}

    # Retrieve basic info. on articles
    cur.execute('select issn, eissn, category from journal;')
    issn_list = cur.fetchall()
    issn_list = issn_list[8000:8100]
    issn_list = [[i[0], i[1], i[2], j] for i in issn_list for j in range(2000, 2020)]
    print('Task assigned')

    # Download article info.
    s = login()
    for journal in tqdm(issn_list, desc='Retrieving articles', mininterval=300):
    	if issn_list.index(journal) >= -1:
            if journal[0] == '':
                journal[0] = journal[1]
            retrieve_article(journal[0], journal[3])
