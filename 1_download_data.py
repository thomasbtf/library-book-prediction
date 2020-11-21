"""
Modul to save MARC21 data from DNB into database
"""

import sqlite3
import pandas as pd
import requests
from xml.etree import ElementTree
from datetime import datetime

def download_dnb_data(isbn, accessToken, connection):
    """
    Method for downloading data from DNB for one isbn in MARC21 format and Stores is in database.
    See https://www.loc.gov/standards/marcxml/ for infos about MARC21
    @params:
        isbn            - Required  : the isbn to down (Int or str)
        accessToken     - Required  : the accestoke to the DNB API (Int or str)
        connection      - Requiered : Connection to the database (e.g. sqlite3.Connection)
    """
    
    #-- catch xml doc from dnb of provided isbn
    url = 'http://services.dnb.de/sru/dnb?version=1.1&operation=searchRetrieve&query=isbn%3D' + str(isbn) + '&recordSchema=MARC21-xml&accessToken=' + str(accessToken)
    r = requests.get(url)

    #-- if request was successfull
    if r.status_code == 200:

        #-- get data of datafields, these contain the MARC21 information
        temp_list = []
        root_of_xml = ElementTree.fromstring(r.content)
        for datafield in root_of_xml.iter():
            if datafield.tag == "{http://www.loc.gov/MARC21/slim}datafield":
                for subfield in datafield:
                    temp_list.append({'ISBN': isbn,
                        'tag': datafield.attrib['tag'],
                        'code': subfield.attrib['code'],
                        'value' : subfield.text
                    })

        #-- if datafields were detected save them to the database
        if len(temp_list)>0:
            #-- Save xml record
            try:
                pd.DataFrame(temp_list).drop_duplicates().to_sql("MARC21", connection, if_exists='append', index=False)
                pd.DataFrame([{'ISBN': isbn, 'MARC21': 1}]).to_sql("Downloaded", connection, if_exists='append', index=False)
            except Exception as e:
                print("%s %s - ISBN %s already recorded" % (datetime.now().strftime("%H:%M"), e, isbn))

        else:
            #-- Set downloaded flag to 0
            try:
                pd.DataFrame([{'ISBN': isbn, 'MARC21': 0}]).to_sql("Downloaded", connection, if_exists='append', index=False)
            except Exception as e:
                print("%s %s - ISBN %s already recorded in Table Downloaded" % (datetime.now().strftime("%H:%M"), e, isbn)) 

    #-- if request was NOT successfull
    else:
        print('%s Got error %s' % (datetime.now().strftime("%H:%M"), r.status_code))

def progressBar(iterable, prefix = 'Progress:', suffix = 'Complete', decimals = 1, length = 100, fill = '█', printEnd = "\r"):
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        length      - Optional  : character length of bar (Int)
        fill        - Optional  : bar fill character (Str)
        printEnd    - Optional  : end character (e.g. "\r", "\r\n") (Str)
    """
    total = len(iterable)

    #-- Progress Bar Printing Function
    def printProgressBar (iteration):
        filledLength = int(length * iteration // total)
        bar = fill * filledLength + '-' * (length - filledLength)
        print(f'\r{prefix} |{bar}| {iteration:,} of {total:,} ({100 * (iteration / float(total)):.1f}%) {suffix}', end = printEnd)

    #-- Initial Call
    printProgressBar(0)

    #-- Update Progress Bar
    for i, item in enumerate(iterable):
        yield item
        printProgressBar(i + 1)

    #--- Print New Line on Complete
    print()

def get_not_downloaded_isbns(all_isbns, downloaded_isbns):
    """
    Identifiy ISBNs, which haven't been downloaded
    @params:
        all_isbns   - Required  : all isbns (pd.Series)
        all_isbns   - Required  : already downlaoded isbns (pd.Series)
    @returns:
        to_download : isbns to download (list)
    """
    all_isbns = all_isbns.to_list()
    downloaded_isbns = downloaded_isbns.to_list()
    to_download = list(set(all_isbns).difference(downloaded_isbns))
    to_download.sort()
    return to_download

#-- connect to database and load data
conn = sqlite3.connect('book_database.db')
catalog = pd.read_sql("SELECT * FROM Books", conn)
downloaded = pd.read_sql("SELECT * FROM Downloaded", conn)

#-- load dnb access token
with open('dnb_key.txt', 'r') as f:
    dnb_access_token = f.read()

#-- Determin isbns to download
isbns_to_download = get_not_downloaded_isbns(catalog.ISBN, downloaded.ISBN)

#-- Download MARC Data from DNB form choosen isbns
for isbn in progressBar(isbns_to_download, prefix='Download Progress'):
    download_dnb_data(isbn, dnb_access_token, conn)