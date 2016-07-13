import urllib
import datetime
import os 
import pdb
from zipfile import ZipFile
import bs4
import json
import numpy as np
import pandas as pd

# reference: https://github.com/lukerosiak/pysec/blob/master/pysec/management/commands/sec_import_index.py

DATA_DIR = './data'
BASE_URL = 'ftp://ftp.sec.gov/'

def is_number(string):
    s = string.replace(',','')
    try: 
        float(s)
        return True
    except ValueError:
        pass

    try: 
        import unicodedata
        unicodedata.numeric(s)
        return True
    except (TypeError, ValueError):
        pass

    return False

def removeNonAscii(s): return "".join(i for i in s if ord(i)<128)

def download_index(year, qtr):
    url=BASE_URL + 'edgar/full-index/{0}/QTR{1}/company.zip'.format(year, qtr)
    fn = '{0}/company_{1}_{2}.zip'.format(DATA_DIR, year, qtr)

    if not os.path.exists(fn):
        compressed_data=urllib.urlopen(url).read()
        fileout=file(fn,'w')
        fileout.write(compressed_data)
        fileout.close()

    # Extract the compressed file
    zip=ZipFile(fn)
    zdata=zip.read('company.idx')
    zdata = removeNonAscii(zdata)

    # Parse the fixed-length fields
    result=[]
    for r in zdata.split('\n')[10:]:
        date = r[86:98].strip()
        if date=='': date = None
        if r.strip()=='': continue
        filing={'name':r[0:62].strip(),
                'form':r[62:74].strip(),
                'cik':r[74:86].strip(),
                'date':date,
                'quarter': qtr,
                'filename':r[98:].strip()}
        if filing['form'] != 'DEF 14A': continue
        result.append(filing)

    return result

def combine_similar_cells(arr, x):
    # combine 2 adjacent columns of white spaces into one column of white space
    # eg [x][x][x] --> [x][x]
    n = len(arr)
    i = 0
    result = []
    while i < n:
        result.append(arr[i])
        if arr[i] != x:
            i+=1
            continue
        else:
            if i + 1 == n: break
            if arr[i+1] != x:
                i+=1
                continue
            else:
                i+=2
                continue
    return result


def valid_table_inputs(tbl):
    #check if every row has the same number of columns
    col_per_row = map(lambda x: len(x), tbl)
    n = len(set(col_per_row))
    return n == 1



def remove_empty_rows(tbl):
    # if a row only has '' then remove the row
    result = []
    for row in tbl:
        s = set(row)
        if len(s) > 1:
            result.append(row)
        if len(s) == 1 and ('' not in s):
            result.append(row)
    return result


def remove_empty_string(row): 
    #remove elements that == '' from a list of strings
    return filter(lambda x: x not in ('', '$'), row)

def remove_empty_cells(tbl):
    result = []
    # assume the header rows have the same number of elements
    # then the data rows have different number of elements
    col_per_row = map(lambda x: len(x), tbl)
    n = next((i for i,v in enumerate(col_per_row) if v!=col_per_row[0]),-1)
    arr = np.array(tbl[:n])
    h_arr = np.transpose(arr)
    header = map(lambda x: x.strip(), [' '.join(row) for row in h_arr])
    result.append(remove_empty_string(header))
    for row in tbl[n:]:
        result.append(remove_empty_string(row))

    col_per_row = map(lambda x: len(x), result)

    pdb.set_trace()
    if len(set(col_per_row)) == 1:
        return result
    else:
        return tbl

def rebuild_table_by_colspan(table_soup):
    #content per colspan
    temp = []
    s = set()
    for tr in table_soup.find_all('tr'):
        row = []
        offset = 0
        for td in tr.find_all('td'):
            try:
                td_col = int(td['colspan'])
            except:
                td_col = 1

            offset += td_col
            txt = clean_tag_text(td.get_text())
            if txt != '':
                s.add(offset)
            row.append((offset, txt))
        temp.append(row)

    result = []
    for row in temp:
        i = []
        for item in row: 
            if item[0] in s:
                i.append(item[1])
        result.append(i)

    return result

def clean_tag_text(txt):
    s = removeNonAscii(txt)
    #remove white spaces
    s = ' '.join(s.split())
    s = s.strip()
    return s


def download_proxy(index): 
    proxy = index[0]
    fn = '{0}/cik_{1}_date_{2}_orig.html'.format(DATA_DIR, proxy['cik'], proxy['date'])
    if not os.path.exists(fn):
        url = BASE_URL + proxy['filename']
        raw = urllib.urlopen(url).read()
        fileout=file(fn, 'w')
        fileout.write(raw)
        fileout.close()
    else:
        raw = file(fn, 'r').read()

    soup = bs4.BeautifulSoup(raw, 'html5lib')
    result = [] 
    fn = '{0}/cik_{1}_date_{2}_extract.html'.format(DATA_DIR, proxy['cik'], proxy['date'])
    fileout=file(fn, 'w')

    for table in soup.find_all('table'):
        #flag for a good table
        is_good_table = False
        #if the first row has more than x columns, then the table might be the right table to look at
        if len(table.tr.find_all('td')) > 4:
            #pdb.set_trace()
            tbl = [] 

            try: 
                table.tr.td['width'] 
                # scenario 1: the rows have "width" element, need to align the columns by width 
                print 'table has width'
            except:

                # scenario 2: the rows have no "width" element, can simply align the columns
                print 'table has no width'

                for tr in table.find_all('tr'):
                    row = []
                    for td in tr.find_all('td'):
                        row.append(clean_tag_text(td.get_text()))
                    tbl.append(row)
                
                # as is, without doing any work
                if valid_table_inputs(tbl):
                    is_good_table = True
                else: 
                    #positiion the columns by colspan
                    if table.find(lambda tag: tag.name == 'td' and tag.has_attr('colspan')) is not None:
                        tbl = rebuild_table_by_colspan(table)
                        if valid_table_inputs(tbl):
                            is_good_table = True

                    if is_good_table == False:
                        #remove the empty rows and try again
                        tbl = remove_empty_rows(tbl)
                        if valid_table_inputs(tbl):
                            is_good_table = True
                        else: 
                            #assume in useful tables every cell is filled up with contents
                            #remove all the '' cells
                            tbl = remove_empty_cells(tbl)
                            if valid_table_inputs(tbl):
                                is_good_table = True

                if is_good_table:
                    result.append(tbl)
                    fileout.write(str(table))
                    fileout.write('\n')
    fileout.close()

    return result


def rebuild_table(orig_tbls):
    pdb.set_trace()
    number = 0
    for tbl in orig_tbls:
        arr = np.array(tbl)
        # if every cell in a column is '', then remove the column from the array
        # if every cell in a row is '', then remove the row from the array
        for axis in range(2):
            idx = np.apply_along_axis(lambda a: reduce(lambda x, y: x and y, a) , axis, (arr == ''))
            if axis == 0: 
                arr = arr[:, ~idx]
            if axis == 1: 
                arr = arr[~idx, :]

        # replace '-' with 0
        arr[arr == '-'] = 0 

        # data rows should contain numerics
        # before we start to see numerics, all rows are part of the header
        idx = np.apply_along_axis(lambda a: reduce(lambda x, y: x or is_number(y), a, False),1,arr)
        n = np.where(idx==True)[0][0]
        h_arr = np.transpose(arr[:n, :])
        header = map(lambda x: x.strip(), [' '.join(row) for row in h_arr])
        d_arr = arr[n:, :]
        df = pd.DataFrame(
                    data = d_arr, #values
                    columns = header
                )
        df.to_csv('table{0}.csv'.format(number), sep='\t')
        number += 1





#def insert_dynamodb(item):
#    #http://neuralfoundry.com/scrapy-and-dynamodb-on-aws/
#    dynamodb = boto3.resource('dynamodb', region_name='us-west-1')
#    table = dynamodb.Table('')
#    table.put_item(
#        Item = {
#            '':str(),
#            '':str(),
#            '':str(),
#        }
#            
#    )
#    return item

    

if __name__ == '__main__':
    #tests 
    #print is_number('3000')
    #print is_number('3,000')
    #print is_number('1e3000')
    #print is_number('-1e3000')
    #print is_number('3,000.25')
    #arr = np.array([['man','100'],['3,000','6.25'],['test','test']])
    #idx = np.apply_along_axis(lambda a: reduce(lambda x, y: x or is_number(y), a, False),1,arr)
    #idx = np.array([False, False, True, False, True, False, True, True]) 
    #n = np.where(idx==True)[0][0]
    

    index = download_index(2015, 4)
    flowers = download_proxy(index)
    rebuild_table(flowers)

    #parse_file()
    

