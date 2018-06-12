import csv
import os.path
import sys
from datetime import datetime

import pymongo
from pymongo import MongoClient
from dateutil import parser


def add_report_date(row, date):
    row['Reporting Period'] = datetime.strptime(date, '%m%d%Y')
    invalid = [key for key in row.keys() if '.' in key]
    for key in invalid:
        if '.' in key:
            row[key.replace('.', '')] = row[key]
            del row[key]
    return row

def modify_balance_sheet(row, date):
    if row['TOTAL ASSETS']:
        row['TOTAL ASSETS'] = int(row['TOTAL ASSETS'])
    return add_report_date(row, date)

def modify_bank_info(row, date):
    row['Last Date/Time Submission Updated On'] = parser.parse(
        row['Last Date/Time Submission Updated On'])
    return add_report_date(row, date)

def load_bank_info(path, date, collection):
    with open(path) as f:
        reader = csv.DictReader(f, delimiter='\t')
        def gen_row():
            for row in reader:
                modified_row = modify_bank_info(row, date)
                yield modified_row
        collection.insert_many(gen_row())

def load_balance_sheet(path, date, collection):
    first = True
    count = 1
    headers = []
    with open(path) as f:
        header = f.readline().split('\t')[0].replace("\"", '')
        headers.append(header)
        headers.extend(f.readline().split('\t'))
        def gen_row():
            for line in f.readlines():
                d = {}
                for k, v in zip(headers, line.split('\t')):
                    d[k] = v
                yield modify_balance_sheet(d, date)
        collection.insert_many(gen_row())

if __name__ == '__main__':
    date = sys.argv[1]
    data_dir = 'data/{}'.format(date)
    # connect to DB
    client = MongoClient('localhost', 27017)
    #client.drop_database('precision_lender')
    db = client['precision_lender']

    # set up indexes
    db['banks'].create_index([('Reporting Period', pymongo.ASCENDING)])
    db['banks'].create_index([('IDRSSD', pymongo.ASCENDING)])
    db['balance_sheet'].create_index([('IDRSSD', pymongo.ASCENDING)])
    db['balance_sheet'].create_index([('Reporting Period', pymongo.ASCENDING)])

    # load data
    bank_info_path = os.path.join(data_dir, 'FFIEC CDR Call Bulk POR {}.txt'.format(date))
    balance_sheet_path = os.path.join(data_dir, 'FFIEC CDR Call Schedule RC {}.txt'.format(date))
    load_bank_info(bank_info_path, date, db['banks'])
    load_balance_sheet(balance_sheet_path, date, db['balance_sheet'])
