import pandas as pd
import xmltodict
import boto3
import glob
import os
from io import BytesIO
from urllib.request import urlopen
from zipfile import ZipFile

HOME_DIR = os.path.dirname(os.path.abspath(__file__)) + "/"
TAGS = ['BizData', 'Pyld', 'Document', 'FinInstrmRptgRefDataDltaRpt', 'FinInstrm']

def parseFile(file):

    final_dicts = []
    data_dict = {}
    try:
        with open(file) as xml_file:
            data_dict = xmltodict.parse(xml_file.read())
        xml_file.close()

        for keys in TAGS:
            data_dict = data_dict.get(keys)

        for data in data_dict:
            temp_dict = {
                'Id': '', 
                'FullNm': '', 
                'ClssfctnTp': '', 
                'CmmdtyDerivInd': '', 
                'NtnlCcy': '',
                'Issr': ''
                }

            for value in data.values():
                for key, val in value.items():
                    if key == 'Issr':
                        temp_dict[key] = val
                    elif key == 'FinInstrmGnlAttrbts':
                        for attrkey, attrval in val.items():
                            if attrkey == 'Id':
                                temp_dict[attrkey] = attrval
                            elif attrkey == 'ClssfctnTp':
                                temp_dict[attrkey] = attrval
                            elif attrkey == 'CmmdtyDerivInd':
                                temp_dict[attrkey] = attrval
                            elif attrkey == 'FullNm':
                                temp_dict[attrkey] = attrval
                            elif attrkey == 'NtnlCcy':
                                temp_dict[attrkey] = attrval

                final_dicts.append(temp_dict)

    except Exception as e:
        print(e)

    return pd.DataFrame(final_dicts)


def download_files():
    download_links = []
    main_xml = HOME_DIR + "s-eye.xml"
    try:
        with open(main_xml) as xml_file:
            data_dict = xmltodict.parse(xml_file.read())

        xml_file.close()

        for docs in data_dict.get('response').get('result').get('doc'):
            for doc in docs.get('str'):

                if doc.get('@name') == 'download_link':
                    link = doc.get('#text', None)
                    if link is not None:
                        download_links.append(link)
    except Exception as e:
        print(e)

    return download_links

def download_unzip( file_list ):

    download_path = HOME_DIR + "downloadData"
    if not os.path.exists(download_path):
        os.makedirs(download_path)

    for file in file_list:
        try:
            with urlopen(file) as zipresp:
                with ZipFile(BytesIO(zipresp.read())) as zfile:
                    zfile.extractall(download_path)
        except Exception as e:
            print(e)

def xml_to_df():
    download_path = HOME_DIR + "downloadData"
    file_pattern_path = "{}/*.xml".format(download_path)

    files = glob.glob(file_pattern_path)

    result = pd.DataFrame()
    for fi in files:
        result = pd.concat([result ,parseFile(fi)])

    return result

def df_to_csv(data_frame):
    outpath = HOME_DIR + "finalcsv/"
    if not os.path.exists(outpath):
        os.makedirs(outpath)

    try:
        out_file = outpath + "final.csv"
        data_frame.to_csv(out_file, index=False)
        print("Process Completed")
    except Exception as e:
        print(e)

    return out_file

def uploadS3(path):

    try:
        session = boto3.Session(profile_name='default')
        s3_resource = session.resource('s3')
        rem_file = "final.csv"
        s3_resource.meta.client.upload_file(path, BUCKET, rem_file)
    except Exception as e:
        print(e)

    print("Uploading complete....")


if __name__ == '__main__':

    links_list = download_files()
    download_unzip(links_list)
    df = xml_to_df()
    path = df_to_csv(data_frame=df)
    uploadS3(path)


        