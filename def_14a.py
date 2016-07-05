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
        #if the first row has more than x columns, then the table might be the right table to look at
        if len(table.tr.find_all('td')) > 4:
            tbl = [] 
            for tr in table.find_all('tr'):
                row = []
                for td in tr.find_all('td'):
                    s = removeNonAscii(td.get_text())
                    #remove white spaces
                    s = ' '.join(s.split())
                    s = s.strip()
                    row.append(s)
                    
                    if len(td.find_all('p'))>0:
                        row.append('')
                tbl.append(combine_similar_cells(row,''))

                pdb.set_trace()

            #verify every list has same number of elements, aka. same number of columns
            col_per_row = map(lambda x: len(x), tbl)
            n = len(set(col_per_row))
            if n == 1:
                result.append(tbl)
                fileout.write(str(table))
                fileout.write('\n')
    fileout.close()

    return result

    #for table in soup.find_all('table'):
    #    for row in table.find_all('tr'):
    #        for cell in row.find_all('td'):



def rebuild_table(orig_tbl):
    for tbl in orig_tbl:
        arr = np.array(tbl)
        # if every cell in a column is '', then remove the column from the array
        # if every cell in a row is '', then remove the row from the array
        for axis in range(2):
            idx = np.apply_along_axis(lambda a: reduce(lambda x, y: x and y, a) , axis, (arr == ''))
            pdb.set_trace()
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



        print 'ok'

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
    flower = download_proxy(index)
    rebuild_table(flower)

    #parse_file()
    

