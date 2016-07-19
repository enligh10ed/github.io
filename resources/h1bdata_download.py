# -*- coding: utf-8 -*-
# This script downloads and restructures work visa (H1B) data from agencies of 
# the U.S. Labor Department. I developed and tested this code on:

# Python 3.5.2 :: Anaconda 4.1.1 (64-bit)
# Linux-3.13.0-74-generic-x86_64-with-debian-jessie-sid
# Number of cpu cores: 8

# As at the time I ran the code, the program output was as follows:
# Number of records downloaded: 5231213 rows and 14 columns
# Program runtime: 14.4 minutes


import ssl
import time
import pickle
import pandas as pd
from csv import reader
import multiprocessing as mp
import requests, zipfile, io
from urllib.request import urlopen

def getXlsxData(url):
    """This function downloads Xlsx H1-B data from the new H1-B system hosted 
    at foreignlaborcert.doleta.gov. The files are available for 2008 to 2015.
    
    Side note: I ran this code on an Amazon 
    EC2 m4.2xlarge Linux instance and was getting an 
    “SSL: CERTIFICATE_VERIFY_FAILED” Error from foreignlaborcert.doleta.gov. 
    I tried various things to get around the error and the only thing that 
    worked is the stackoverflow suggestion here: 
    http://stackoverflow.com/a/28052583/3594865. If you've a better idea, 
    I'd love to hear it!
"""
    
    ssl._create_default_https_context = ssl._create_unverified_context
    context = ssl._create_unverified_context()
    response = urlopen(url, context=context)
    xl = pd.ExcelFile(response)
    sheet_names = xl.sheet_names
    
    currentDF = xl.parse(sheet_names[0])
    
    if sheet_names[0] == "H-1B_Case_Data_FY2009":
        currentDF.drop(currentDF.columns[[2,4,5,8,10,11,12,14,15,16,20,25,26,27,28,29,
                                          30,31,32,33,34,35,36, 37, 38]], axis=1, inplace=True)
    
    elif sheet_names[0] == "H-1B_Case_Data_FY2008":
        currentDF.drop(currentDF.columns[[2,4,5,9,10,11,13,14,15,16,20,25,26,27,28,29,
                                          30,31,32,33,34,35,36,37,38, 39]], axis=1, inplace=True)
    
    elif sheet_names[0] == "H-1B_FY2015":
        currentDF.drop(currentDF.columns[[3,4,5,6,8,9,13,14,15,16,17,18,19,21,22,23,24,
                                          28,29,30,31,34, 35, 37, 39]], axis=1, inplace=True)
        
        # Create an empty data frame to use in rearranging data frame columns
        tempDF = pd.DataFrame()
        
        tempDF["Submitted_Date"] = currentDF['CASE_SUBMITTED']
        tempDF["Case_Number"] = currentDF['CASE_NUMBER']
        tempDF["Name"] = currentDF['EMPLOYER_NAME']
        tempDF["City"] = currentDF['EMPLOYER_CITY']
        tempDF["State"] = currentDF['EMPLOYER_STATE']
        tempDF["Postal_Code"] = currentDF['EMPLOYER_POSTAL_CODE']
        tempDF["Job_Title"] = currentDF['JOB_TITLE']
        tempDF["Approval_Status"] = currentDF['CASE_STATUS']
        tempDF["Wage_Rate"] = currentDF['WAGE_RATE_OF_PAY']
        tempDF["Wage_Rate_Unit"] = currentDF['WAGE_UNIT_OF_PAY']
        tempDF["Part_Time"] = currentDF['FULL_TIME_POSITION']
        tempDF["Work_City"] = currentDF['WORKSITE_CITY']
        tempDF["Work_State"] = currentDF['WORKSITE_STATE']
        tempDF["Prevailing_Wage"] = currentDF['PREVAILING_WAGE']
        
        currentDF = tempDF
        
    elif sheet_names[0] == "H1B_FY2010":
        currentDF.drop(currentDF.columns[[3,4,5,7,8,12,13,16,17,22,23,24,25,26,27,28,29,
                                          30,31,32]], axis=1, inplace=True)
        currentDF["FULL_TIME_POS"] = None
        
        # Create an empty data frame to use in rearranging data frame columns
        tempDF = pd.DataFrame()
        
        tempDF["Submitted_Date"] = currentDF['LCA_CASE_SUBMIT']
        tempDF["Case_Number"] = currentDF['LCA_CASE_NUMBER']
        tempDF["Name"] = currentDF['LCA_CASE_EMPLOYER_NAME']
        tempDF["City"] = currentDF['LCA_CASE_EMPLOYER_CITY']
        tempDF["State"] = currentDF['LCA_CASE_EMPLOYER_STATE']
        tempDF["Postal_Code"] = currentDF['LCA_CASE_EMPLOYER_POSTAL_CODE']
        tempDF["Job_Title"] = currentDF['LCA_CASE_JOB_TITLE']
        tempDF["Approval_Status"] = currentDF['STATUS']
        tempDF["Wage_Rate"] = currentDF['LCA_CASE_WAGE_RATE_FROM']
        tempDF["Wage_Rate_Unit"] = currentDF['PW_UNIT_1']
        tempDF["Part_Time"] = currentDF['FULL_TIME_POS']
        tempDF["Work_City"] = currentDF['WORK_LOCATION_CITY1']
        tempDF["Work_State"] = currentDF['WORK_LOCATION_STATE1']
        tempDF["Prevailing_Wage"] = currentDF['PW_1']
        
        currentDF = tempDF
        
    else:
        currentDF.drop(currentDF.columns[[3,4,5,6,8,12,13,16,19,24,25,26,27,28,29,30,31,
                                          32,33,34]], axis=1, inplace=True)
        
        # Create an empty data frame to use in rearranging data frame columns
        tempDF = pd.DataFrame() 
        
        tempDF["Submitted_Date"] = currentDF['LCA_CASE_SUBMIT']
        tempDF["Case_Number"] = currentDF['LCA_CASE_NUMBER']
        tempDF["Name"] = currentDF['LCA_CASE_EMPLOYER_NAME']
        tempDF["City"] = currentDF['LCA_CASE_EMPLOYER_CITY']
        tempDF["State"] = currentDF['LCA_CASE_EMPLOYER_STATE']
        tempDF["Postal_Code"] = currentDF['LCA_CASE_EMPLOYER_POSTAL_CODE']
        tempDF["Job_Title"] = currentDF['LCA_CASE_JOB_TITLE']
        tempDF["Approval_Status"] = currentDF['STATUS']
        tempDF["Wage_Rate"] = currentDF['LCA_CASE_WAGE_RATE_FROM']
        tempDF["Wage_Rate_Unit"] = currentDF['PW_UNIT_1']
        tempDF["Part_Time"] = currentDF['FULL_TIME_POS']
        tempDF["Work_City"] = currentDF['LCA_CASE_WORKLOC1_CITY']
        tempDF["Work_State"] = currentDF['LCA_CASE_WORKLOC1_STATE']
        tempDF["Prevailing_Wage"] = currentDF['PW_1']
        
        currentDF = tempDF
    print("{0}: {1} rows".format(sheet_names[0], len(currentDF)))
    return currentDF.values.tolist()
    

def getZippedData(url):
    """This function downloads H1B files from the old H1-B system hosted at
    at flcdatacenter.com.The files are available for 2002 to 2007. """
    
    r = requests.get(url)
    zf = zipfile.ZipFile(io.BytesIO(r.content))
    all_lines = []
    for name in zf.namelist():
        if name.endswith(".txt"):
            currentFile = []
            stream = None
            stream = zf.open(name)    
        # Get the contents of the unzipped file 
            for line in stream:
                string = line.decode(encoding='windows-1252')
                currentFile.append(string)
            all_lines = all_lines + currentFile[1:]
            print("{0}: {1} rows".format(name, len(currentFile)))
    
    clean = []
         
##   Using reader() below helps to avoid splitting the data on inner commas.
##   E.g., Google, Inc. will not be splitted into two
    for item in all_lines:
        thisList = []
        for line in reader(item.split(",")): 
            try:
                thisList.append(line[0])
            except IndexError:
                thisList.append("")  
        clean.append(thisList)
    currentDF = pd.DataFrame(clean)
            
    try:
        currentDF.drop(currentDF.columns[[2,4,5,9,10,11,13,14,15,16,20,25,26,27,28,29,30,31,32,
                                                  33,34,35,36,37,38]], axis=1, inplace=True)
    except (AttributeError, KeyError, IndexError) as e:
        currentDF.drop(currentDF.columns[[3,4,8,9,10,12,13,14,15,19,24,25,26,27,28,29,30,31,32,
                                                  33,34,35,36]], axis=1, inplace=True)  
    return currentDF.values.tolist()


def collect_results(result):
    """Uses Python multiprocessing apply_async's callback to 
    setup up a separate Queue for each process"""
    results.extend(result)

if __name__ == "__main__":

	# Define the data sources
    urls = ["http://www.flcdatacenter.com/download/H1B_efile_FY07_text.zip",
            "http://www.flcdatacenter.com/download/H1B_efile_FY06_text.zip",
            "http://www.flcdatacenter.com/download/H1B_efile_FY04_text.zip",
            "http://www.flcdatacenter.com/download/H1B_efile_FY03_text.zip",
            "http://www.flcdatacenter.com/download/H1B_efile_FY02_text.zip",
            "https://www.foreignlaborcert.doleta.gov/docs/lca/H-1B_Case_Data_FY2008.xlsx",
            "https://www.foreignlaborcert.doleta.gov/docs/lca/H-1B_Case_Data_FY2009.xlsx",
            "https://www.foreignlaborcert.doleta.gov/docs/lca/Icert_%20LCA_%20FY2009.xlsx",
            "https://www.foreignlaborcert.doleta.gov/docs/lca/H-1B_FY2010.xlsx",
            "https://www.foreignlaborcert.doleta.gov/docs/lca/H-1B_iCert_LCA_FY2011_Q4.xlsx",
            "https://www.foreignlaborcert.doleta.gov/docs/py2012_q4/LCA_FY2012_Q4.xlsx",
            "https://www.foreignlaborcert.doleta.gov/docs/lca/LCA_FY2013.xlsx",
            "https://www.foreignlaborcert.doleta.gov/docs/py2014q4/H-1B_FY14_Q4.xlsx",
            "https://www.foreignlaborcert.doleta.gov/docs/py2015q4/H-1B_Disclosure_Data_FY15_Q4.xlsx"]
    
	# Define a list to collect the content from each data source
    results = []
    
	# Define the headers of the restructured
    new_headers =  ["Submitted_Date", 
                        "Case_Number", 
                        "Employer_Name", 
                        "Employer_City", 
                        "Employer_State", 
                        "Employer_Postal_Code", 
                        "Job_Title", 
                        "Approval_Status",
                        "Wage_Rate", 
                        "Wage_Rate_Unit", 
                        "Part_Time", 
                        "Work_City", 
                        "Work_State", 
                        "Prevailing_Wage" ]
    
    # The download takes quite a long time. Parallelizing it helps!
    start_time = time.time()
    pool = mp.Pool(processes=mp.cpu_count())  
    for url in urls:
        if url.endswith(".zip"):
            pool.apply_async(getZippedData, args=(url, ), callback=collect_results)
        else:
            pool.apply_async(getXlsxData, args=(url, ), callback=collect_results)
    pool.close()
    pool.join()
    
    # Merge the data frames
    h1bdataDF = pd.DataFrame(results, columns=new_headers)
    pickle.dump(h1bdataDF, open('h1bdataDF.pkl', 'wb'))
    print("h1bdataDF: {0}".format(h1bdataDF.shape))
    print("Time to read and restructure DOL files --- %s minutes ---" % ((time.time() - start_time)/60))  