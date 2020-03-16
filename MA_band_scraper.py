import time
import datetime
import requests
from bs4 import BeautifulSoup
from pandas import DataFrame

BASEURL = 'http://www.metal-archives.com'
headers = {'User-agent': 'Mozilla/5.0'}
response_len = 500


def get_bands(letter='A', start=0, length=500):
    """
    Gets the listings displayed as alphabetical tables on Metal Archives for
    input `letter`, starting at `start` and ending at `start` + `length`.
    Returns a `Response` object. Data can be accessed by callingt the `json()`
    method of the returned `Response` object.
    """

    RELURL = '/browse/ajax-letter/l/'
    URL_SUFFIX = '/json/1/'

    payload = {'sEcho': 0,  # if not set, response text is not valid JSON
               'iDisplayStart': start,  # start index of band names returned
               'iDisplayLength': length}  # only response lengths of 500 work

    request = requests.get(BASEURL + RELURL + letter + URL_SUFFIX,
                           params=payload,
                           headers=headers)

    return request.json()


def clean_data(data):
    """
    Gets the messed request data and separate data into proper fields, such
    as `Name` and `Link` instead of `NameLink`, which is in fact only a HTML
    div field. Returns a cleaned DataFrame object.
    """
    data['NameSoup'] = data['NameLink'].map(
        lambda raw_html: BeautifulSoup(raw_html, 'html.parser'))
    data['Name'] = data['NameSoup'].map(lambda soup: soup.text)
    data['Link'] = data['NameSoup'].map(lambda soup: soup.a['href'])

    data['StatusSoup'] = data['Status'].map(
        lambda raw_html: BeautifulSoup(raw_html, 'html.parser'))
    data['Status'] = data['StatusSoup'].map(lambda soup: soup.text)

    data['BandID'] = data['Link'].map(lambda link: link.rsplit('/', 1)[-1])

    # data.drop(['NameLink', 'NameSoup', 'StatusSoup'])

    return data


def get_albuns(band_id='25'):
    """
    Gets a band ID and lists all up-to-date releases of the given band.
    Returns a list of albuns.
    """
    RELURL = '/band/discography/id/'
    URL_SUFFIX = '/tab/all/'
    request = requests.get(BASEURL + RELURL + band_id + URL_SUFFIX,
                           headers=headers)
    return request


column_names = ['NameLink', 'Country', 'Genre', 'Status']
data = DataFrame()

letters = list(map(chr, range(65, 91)))
letters.extend(['NBR', '~'])
date_of_scraping = datetime.datetime.utcnow().strftime('%Y-%m-%d')

for letter in letters:
    print('Current letter =', letter)
    request = get_bands(letter=letter, start=0, length=response_len)
    n_records = request['iTotalRecords']
    n_chunks = int(n_records / response_len) + 1
    print('Total records =', n_records)

    for i in range(n_chunks):
        start = response_len * i
        if start + response_len < n_records:
            end = start + response_len
        else:
            end = n_records
        print('Fetching band entries', start, 'to', end)

        for attempt in range(10):
            time.sleep(3)
            try:
                request = get_bands(letter=letter,
                                    start=start,
                                    length=response_len)
                df = DataFrame(request['aaData'])
                data = data.append(df)
            except Exception as e:
                print('JSON decode error on attempt', attempt, 'of 10.')
                print('Retrying...')
                continue
            break

data.columns = column_names
data.index = range(len(data))

print('Cleaning data...')
data = clean_data(data)

f_name = 'MA-band-names_{}.csv'.format(date_of_scraping)
print('Writing band data to csv file:', f_name)
data.to_csv(f_name)
print('Complete!')
