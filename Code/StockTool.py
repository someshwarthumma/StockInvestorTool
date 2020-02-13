
# coding: utf-8

# In[10]:


import os  
from selenium import webdriver  
from selenium.webdriver.common.keys import Keys  
from selenium.webdriver.chrome.options import Options  
import unittest
import multiprocessing as mp
from multiprocessing import Pool
import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from selenium.common.exceptions import StaleElementReferenceException
from selenium.common.exceptions import NoSuchElementException,TimeoutException,WebDriverException
import time
import shutil
import os,pickle
import datetime
from datetime import tzinfo, timedelta, datetime, date
import logging
import numpy
import copyreg
import types


# In[11]:


#BSELOFURL abbreviates to Listed Companies | List of Security | BSE URL
BSELOCURL='https://www.bseindia.com/corporates/List_Scrips.aspx'
#BSESHURL abbreviates to Stock History URL
BSESHURL='https://www.bseindia.com/markets/equity/EQReports/StockPrcHistori.aspx?flag=0#/'
#BSESCAURL abbreviates to Stock Corporate Action URL
BSESCAURL='https://www.bseindia.com/corporates/corporate_act.aspx'
#BSEIADataURL abbreviates to Index Archive data URL
BSEIADataURL='https://www.bseindia.com/indices/IndexArchiveData.html'
#BSEBCURL abbreviates to Bhav Copy URL
BSEBCURL='https://www.bseindia.com/markets/MarketInfo/BhavCopy.aspx'
#BSEGSDURL abbreviates to Historical Gross Deliverables URL
BSEGSDURL='https://www.bseindia.com/markets/equity/EQReports/GrossDeliverables.aspx'


# In[12]:


def CheckForNecessaryDirectories(FD=str(os.path.expanduser('~'))):
    if not os.path.isdir(FD+'BSEStocksDailyHistory/'):
        try:
            os.makedirs(FD+'BSEStocksDailyHistory/')
            print("makedirs")
        except OSError:  
            print ("Creation of the directory %s failed" % FD+'BSEStocksDailyHistory/')
    if not os.path.isdir(FD+'ActiveStocks/'):
        try:
            os.makedirs(FD+'ActiveStocks/')
        except OSError:  
            print ("Creation of the directory %s failed" % FD+'ActiveStocks/')
    if not os.path.isdir(FD+'ActiveStocks/CorporateActions/'):
        try:
            os.makedirs(FD+'ActiveStocks/CorporateActions/')
        except OSError:  
            print ("Creation of the directory %s failed" % FD+'ActiveStocks/CorporateActions/')
    if not os.path.isdir(FD+'CorporateActions/'):
        try:
            os.makedirs(FD+'CorporateActions/')
        except OSError:  
            print ("Creation of the directory %s failed" % FD+'CorporateActions/')


# In[13]:


def setWebDriver(Dest_Data_Folder,url):
    #Path_to_Chromedriver=findChromeDriverPath('chromedriver','/home/hadoop/Documents/')
    #/usr/lib/chromium-browser/.
    #Path_to_Chromedriver=findChromeDriverPath('chromedriver','/usr/lib/chromium-browser/)
#     print("In -> setWebDriver",Dest_Data_Folder)
    Path_to_Chromedriver=findChromeDriverPath('chromedriver','/home/somesh/MSIT/Softwares/')
    chrome_options = Options()
    prefs = {"download.default_directory": Dest_Data_Folder, "download.prompt_for_download": False,}
    chrome_options.add_experimental_option("prefs",prefs)
    chrome_options.add_argument("--headless")
#     chrome_options.add_argument('window-size=1200x600') # optional
    chrome_options.add_argument("--kiosk")
    chrome_options.add_argument('--disable-gpu')
    driver = webdriver.Chrome(executable_path=Path_to_Chromedriver,chrome_options=chrome_options)
    driver.command_executor._commands["send_command"] = ("POST", '/session/$sessionId/chromium/send_command')
    # Changing the chrome downloads from default directory to Destination directory
    params = {'cmd': 'Page.setDownloadBehavior', 'params': {'behavior': 'allow', 'downloadPath':Dest_Data_Folder}}
    driver.execute("send_command", params)
    driver.delete_all_cookies()
    driver.get(url) 
    return driver


# In[14]:


def findChromeDriverPath(name='chromedriver', path=str(os.path.expanduser('~'))):
    #def findChromeDriverPath(name='chromedriver', path='/Users/arunkp/'):
#     path='/home/somesh'
#     print(path)
    try:
        isFind=False
        for root, dirs, files in os.walk(path):
            if name in files:
                isFind=True
#                 print("Chrome Driver Path Found")
                return os.path.join(root, name)
        if not isFind:
            print('chromedriver not installed')
            input()
            return
    except OSError:  
            print ('path does not exist')


# In[15]:


def downloadBSEActiveStocksList(FD=str(os.path.expanduser('~')),driver=None):
    #Check for download directory exist, if not create
    if not os.path.isdir(FD+'ActiveStocks/'):
        CheckForNecessaryDirectories(FD)
    #Instantiate a Web Driver assigning chrome downloads folder to Destination directory and send driver request to BSELOCURL
    if driver==None:
        #Instantiate a Web Driver assigning chrome downloads folder to Destination directory and send driver request to BSESHURL
        driver=setWebDriver(FD+'ActiveStocks/',BSELOCURL)
    try:
        if os.path.isfile(FD+'ActiveStocks/Equity.csv'):
            os.remove(FD+'ActiveStocks/Equity.csv')
        #Get the segment form
        segment=driver.find_element_by_xpath('//*[@id="ContentPlaceHolder1_ddSegment"]')
        #Select Equity for segment
        selecttype=Select(segment)
        selecttype.select_by_visible_text('Equity') 
        #Get the status form
        status=driver.find_element_by_xpath('//*[@id="ContentPlaceHolder1_ddlStatus"]')
        #Select Active for status
        selecttype=Select(status)
        selecttype.select_by_visible_text('Active') 
        #Get submit button and click
        driver.find_element_by_xpath('//*[@id="ContentPlaceHolder1_btnSubmit"]').click()
        #Get the download button and click
        driver.find_element_by_xpath('//*[@id="ContentPlaceHolder1_lnkDownload"]/i').click()
        #Wait till the file is downloaded before going to run other script.
        while(not os.path.isfile(FD+'ActiveStocks/Equity.csv')):
            time.sleep(0.5)
        driver.close()
    except (StaleElementReferenceException,NoSuchElementException,WebDriverException) as e:
#         print("Downloading Active stocks ", e)
        driver.get(BSELOCURL)
        downloadBSEActiveStocksList(FD,driver)


# In[35]:


#Download Historical Data for an input list of Security Codes.
def DownLoadListOfBSEStocksDailyHistoricalData(List_Of_Security_Codes=None,Day_From=1,Month_From='Jan',
                                               Year_From=1950,Day_To=datetime.strftime(date.today(), '%d'),
                                               Month_To=datetime.strftime(date.today(), '%b'),
                                               Year_To=datetime.strftime(date.today(), '%Y'), 
                                               Min_Num_Days_History=28,FD=str(os.path.expanduser('~')),npstocks=list()):
#     print("In -> DownLoadListOfBSEStocksDailyHistoricalData -> FD: ",FD)
#     print("List_Of_Security_Codes: ",List_Of_Security_Codes)
    Stocks_failed_to_be_downloaded=[]
    driver=setWebDriver(FD+'BSEStocksDailyHistory/',BSESHURL)
    #driver.implicitly_wait(10) # seconds    
    #npstocks=list()
    Year_From=int(Year_From)
    Year_To=int(Year_To)
    if List_Of_Security_Codes is None:
        List_Of_Security_Codes=GetValidBSEActiveStocksListDF(FD)
#     print("List_Of_Security_Codes: ",List_Of_Security_Codes)
    #Call DownLoadBSEStockDailyHistoricalData for each stock in List_Of_Security_Codes
    for stock in List_Of_Security_Codes:
        try:
            SC=DownLoadBSEStockDailyHistoricalData(str(stock),Day_From,Month_From,Year_From, 
                                                Day_To,Month_To, Year_To,Min_Num_Days_History,driver,FD, npstocks)
            if SC==None:
                Stocks_failed_to_be_downloaded.append(stock)
                #print(stock," Not Downloaded ")
                driver.quit()
                driver=setWebDriver(FD+'BSEStocksDailyHistory/',BSESHURL)
            '''else:
                print(stock,'Downloaded ')'''
            
        except() as e:
            print("Exception ", stock,e)
            driver.quit()
            driver=setWebDriver(FD+'BSEStocksDailyHistory/',BSESHURL)
            '''Stocks_failed_to_be_downloaded.append(stock)
            if not os.path.isfile(FD+'BSEStocksDailyHistory/'+str(stock)+'.csv'):
                driver.quit()
                driver=setWebDriver(FD+'BSEStocksDailyHistory/',BSESHURL)'''
    driver.quit()
#     print('SFTBD ',Stocks_failed_to_be_downloaded)
    '''if len(Stocks_failed_to_be_downloaded)>0:
        DownLoadListOfBSEStocksDailyHistoricalData(Stocks_failed_to_be_downloaded,Day_From,Month_From,
                                                   Year_From,Day_To,Month_To,Year_To,Min_Num_Days_History,FD,npstocks)'''
    print(npstocks)
    return npstocks


# In[17]:


# FD='/home/somesh/MSIT/MSIT 2nd Year/Practicum/'
# #FD='/home/hadoop/Documents/EquityAnalysis/Parallel/'
# # npstocks=list()
# # CheckForNecessaryDirectories(FD)
# # FD=str(os.path.expanduser('~'))
# # print(FD)
# driver=setWebDriver(FD+'BSEStocksDailyHistory/',BSESHURL)

# # DownLoadBSEStockDailyHistoricalData(str(540133),Day_From,Month_From,Year_From,Day_To,Month_To,Year_To,Min_Num_Days_History,driver,FD+'BSEStocksDailyHistory/',npstocks)
# DownLoadListOfBSEStocksDailyHistoricalData(None,1,'Jan',1950,
#                                            datetime.strftime(date.today(), '%d'),
#                                            datetime.strftime(date.today(), '%b'),
#                                            datetime.strftime(date.today(), '%Y'),28,FD,list())
# # GetValidBSEActiveStocksListDF(FD)


# In[18]:


def GetValidBSEActiveStocksListDF(FD=str(os.path.expanduser('~'))):
    BSESecurityCodes=GetBSEActiveStocksListDF(FD)
    #Remove already downloaded stocksS
    npstocks=getListOfStocksAlreadyDownloaded(FD+'BSEStocksDailyHistory/')
    #Add not processable security codes to remove from Active security codes
    [npstocks.append(elt) for elt in getnonProcessableStocks(FD)]
    ValidBSEStocks=[stock for stock in BSESecurityCodes if stock not in npstocks]
    return ValidBSEStocks


# In[19]:


def GetBSEActiveStocksListDF(FD=str(os.path.expanduser('~'))):
    data=pd.read_csv(FD+'ActiveStocks/Equity.csv')
    BSESecurityCodes=data['Security Code']
    return BSESecurityCodes


# In[20]:


def getnonProcessableStocks(FD):
    npstocks=list()
    if os.path.isfile(FD+'ActiveStocks/NPScrips.csv'):
        with open(FD+'ActiveStocks/NPScrips.csv') as f:
            for line in f:
                npstocks.append(int(line.strip()))
    return npstocks


# In[21]:


def DownLoadBSEStockDailyHistoricalData(Security_Code,Day_From=1,Month_From='Jan',Year_From=1900, 
                                        Day_To=datetime.strftime(date.today(), '%d'),
                                        Month_To=datetime.strftime(date.today(), '%b'),
                                        Year_To=datetime.strftime(date.today(), '%Y'), 
                                        Min_Num_Days_History=28, driver=None, FD=str(os.path.expanduser('~')),invalidstocks=None):
#     print("In Individual stock down-> DownLoadBSEStockDailyHistoricalData -> FD: ",FD)
    BSESHURL='https://www.bseindia.com/markets/equity/EQReports/StockPrcHistori.aspx?flag=0#/'
    #Initializing driver
    toClose=False
    if driver==None:
        #Instantiate a Web Driver assigning chrome downloads folder to Destination directory and send driver request to BSESHURL
        driver=setWebDriver(FD+'BSEStocksDailyHistory/',BSESHURL)
        toClose=True
    Year_From=int(Year_From)
    Year_To=int(Year_To)
    #Click the radio button Equity
    if clickButton(driver,'ContentPlaceHolder1_rad_no1') is False:
       return
    #Click the Daily button Equity
    if clickButton(driver,'ContentPlaceHolder1_rad_no1') is False:
       return
    stock=sendSecurityCode(driver, Security_Code,'ContentPlaceHolder1_smartSearch')
    if stock is False:
        return 
    if (Security_Code != stock[0].text.split(' ')[-1]):
        invalidstocks.append(Security_Code)
        print(Security_Code, 'not exist')
        return Security_Code
        #Select and Click From date box
    try:
       stock[0].click()
    except:
        return
    if selectFromDate(driver,'ContentPlaceHolder1_txtFromDate',Day_From,Month_From,Year_From) is False:
        return
        
    if selectToDate(driver,'ContentPlaceHolder1_txtToDate',Day_To,Month_To,Year_To) is False:
        return
    if clickSubmitButton(driver,'ContentPlaceHolder1_btnSubmit') is False:
        return
    table_row_count=getNumberOfTableRows(driver,'//*[@id="ContentPlaceHolder1_spnStkData"]/table/tbody/tr')
    if table_row_count is False:
        return
    if table_row_count<Min_Num_Days_History:
        invalidstocks.append(Security_Code)
        print(Security_Code,'less data')
        return Security_Code
    if os.path.isfile(FD+'BSEStocksDailyHistory/'+Security_Code+'.csv'):
        os.remove(FD+'BSEStocksDailyHistory/'+Security_Code+'.csv')
    if clickDownloadButton(driver,'ContentPlaceHolder1_btnDownload1') is False:
        return
    for i in range(20):
        if not os.path.isfile(FD+str(Security_Code)+'.csv'):
            time.sleep(0.5)
        else:
            driver.close()
            return Security_Code
            break
    if toClose:
       driver.close()
    return


# In[22]:


#stock Historic Data
def runParallelSHDownload(FD,pc,stocks):
    npstocks=[]
    print(len(stocks))
    print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    for i in range(pc):
        npstocks.append([]) 
    result=[]
    factor=len(stocks)//pc
    stockargs=[(stocks[i*factor:(i+1)*factor],1,'Jan',1950,datetime.strftime(date.today(), '%d'), 
                datetime.strftime(date.today(), '%b'),datetime.strftime(date.today(), '%Y'), 28,
                FD,npstocks[i]) for i in range(pc-1)]
    stockargs.append((stocks[(pc-1)*factor:len(stocks)],1,'Jan',1950,datetime.strftime(date.today(), '%d'), 
                datetime.strftime(date.today(), '%b'),datetime.strftime(date.today(), '%Y'), 28,FD,npstocks[pc-1]))
    non_processing_Stocks=getnonProcessableStocks(FD)
    with Pool(pc) as p:
        result=p.starmap(DownLoadListOfBSEStocksDailyHistoricalData, stockargs)
        result.append(non_processing_Stocks)
        with open(FD+'ActiveStocks/NPScrips.csv', 'w') as f:
            for elt in result:
                for scrip in elt:
                    f.write(str(scrip)+'\n')
    print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))


# In[23]:


def getListOfStocksAlreadyDownloaded(dirName=str(os.path.expanduser('~'))+'BSEStocksDailyHistory/'):
    # create a list of file and sub directories 
    # names in the given directory 
    listOfFile = os.listdir(dirName)
    allFiles = list()
    # Iterate over all the entries
    for entry in listOfFile:
        # Create full path
        fullPath = os.path.join(dirName, entry)
        # If entry is a directory then get the list of files in this directory 
        if os.path.isfile(fullPath):
            try:
                if len(entry.split('.'))>2:
                    os.remove(fullPath)
                else:
                    allFiles.append(int(entry.split('.')[0]))
            except:
                pass
            finally:
                pass
    return allFiles


# In[24]:


def clickButton(driver, link):
    try:
        wait = WebDriverWait(driver, 10)
        Equity_Button = wait.until(EC.element_to_be_clickable((By.ID, link)))
        Equity_Button.click()
        return True
    except:
        return False


# In[25]:


def clickSubmitButton(driver,link):
    try:
        wait = WebDriverWait(driver, 10)
        submit_button=wait.until(EC.visibility_of_element_located((By.ID,link)))
        submit_button.click()
        return True
    except:
        return False


# In[26]:


def clickDownloadButton(driver,link):
    try:
        wait = WebDriverWait(driver, 10)
        download_button=wait.until(EC.visibility_of_element_located((By.ID,link)))
        download_button.click()
        return True
    except:
        return False


# In[27]:


def selectFromDate(driver,web_form_link,Day_From,Month_From,Year_From):
    try:
        wait = WebDriverWait(driver, 10)
        datepicker_from=wait.until(EC.visibility_of_element_located((By.ID, web_form_link)))
        datepicker_from.click()
        year_from=driver.find_element_by_xpath("//div/select[@class='ui-datepicker-year']")
        select_from_year=Select(year_from)
        while(int(select_from_year.options[0].text)>Year_From):
            select_from_year.options[0].click()
            year_from=driver.find_element_by_xpath("//div/select[@class='ui-datepicker-year']")
            select_from_year=Select(year_from)
        select_from_year.select_by_visible_text(str(Year_From))
        month_from=driver.find_element_by_xpath("//div/select[@class='ui-datepicker-month']")
        select_from_month=Select(month_from)
        select_from_month.select_by_visible_text(Month_From)
        day_from=driver.find_element_by_xpath("//table/tbody/tr/td/a[text()="+str(Day_From)+"]")
        day_from.click()
        return True
    except:
        return False


# In[28]:


def selectToDate(driver,web_form_link,Day_To,Month_To,Year_To):
    try:
        wait = WebDriverWait(driver, 10)
        datepicker_from=wait.until(EC.visibility_of_element_located((By.ID, web_form_link)))
        datepicker_from.click()        
        year_to=driver.find_element_by_xpath("//div/select[@class='ui-datepicker-year']")
        select_to_year=Select(year_to)
        while(int(select_to_year.options[0].text)>Year_To):
            select_to_year.options[0].click()
            year_to=driver.find_element_by_xpath("//div/select[@class='ui-datepicker-year']")
            select_to_year=Select(year_to)
        select_to_year.select_by_visible_text(str(Year_To))
        month_to=driver.find_element_by_xpath("//div/select[@class='ui-datepicker-month']")
        select_to_month=Select(month_to)
        select_to_month.select_by_visible_text(Month_To)
        day_to=driver.find_element_by_xpath("//table/tbody/tr/td/a[text()="+str(Day_To)+"]")
        day_to.click()
        return True
    except() as e:
        print(e)
        return False


# In[29]:


def sendSecurityCode(driver, Security_Code, link):
    try:
        wait = WebDriverWait(driver, 10)
        ScripBox=wait.until(EC.visibility_of_element_located((By.ID,link)))
        ScripBox.clear()
        ScripBox.send_keys(Security_Code)
        scrip_options=driver.find_elements_by_id('ulSearchQuote2')
        if len(scrip_options)==0:
            return False
        return scrip_options
    except:
        return False


# In[30]:


def getNumberOfTableRows(driver,link):
    try:
        table=driver.find_elements_by_xpath(link)
        table_row_count=len(table)
        return table_row_count
    except:
        return False


# In[31]:


def getListOfStocksForCorporateActionsAlreadyDownloaded(dirName=str(os.path.expanduser('~'))+'CorporateActions/'):
    # create a list of file and sub directories 
    # names in the given directory 
    listOfFile = os.listdir(dirName)
    allFiles = list()
    # Iterate over all the entries
    for entry in listOfFile:
        # Create full path
        fullPath = os.path.join(dirName, entry)
        # If entry is a directory then get the list of files in this directory 
        if os.path.isfile(fullPath+'/Corporate_Actions.csv'):
            try:
                allFiles.append(int(entry))
            except:
                pass
            finally:
                pass
    return allFiles


# In[32]:


def runParallelCADownload(FD,pc,stocks):
    npstocks=[]
    print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    for i in range(pc):
        npstocks.append([]) 
    result=[]
    factor=len(stocks)//pc
    stockargs=[(stocks[i*factor:(i+1)*factor],1,'Jan',1950,datetime.strftime(date.today(), '%d'), 
                datetime.strftime(date.today(), '%b'),datetime.strftime(date.today(), '%Y'),
                FD,npstocks[i]) for i in range(pc-1)]
    stockargs.append((stocks[(pc-1)*factor:len(stocks)],1,'Jan',1950,datetime.strftime(date.today(), '%d'), 
                datetime.strftime(date.today(), '%b'),datetime.strftime(date.today(), '%Y'), FD,npstocks[pc-1]))
    non_processing_Stocks=getnonProcessableStocks(FD)
    with Pool(pc) as p:
        result=p.starmap(DownLoadSecurityWiseCorporateActionsBSE, stockargs)
        result.append(non_processing_Stocks)
        with open(FD+'ActiveStocks/CorporateActions/NPScrips.csv', 'w') as f:
            for elt in result:
                for scrip in elt:
                    f.write(str(scrip)+'\n')
    print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))


# In[42]:


def DownLoadSecurityWiseCorporateActionsBSE(List_Of_Security_Codes=None,Day_From=1,Month_From='Jan',
                                               Year_From=1950,Day_To=datetime.strftime(date.today(), '%d'),
                                               Month_To=datetime.strftime(date.today(), '%b'),
                                               Year_To=datetime.strftime(date.today(), '%Y'), 
                                               FD=str(os.path.expanduser('~')),npstocks=list()):
    Stocks_failed_to_be_downloaded=[]
    #driver=setWebDriver(FD+'CorporateActions/',BSESCAURL)
    #driver.implicitly_wait(10) # seconds    
    #npstocks=list()
    Year_From=int(Year_From)
    Year_To=int(Year_To)
    if List_Of_Security_Codes is None:
        List_Of_Security_Codes=getListOfStocksAlreadyDownloaded(FD+'BSEStocksDailyHistory/')
    #Call DownLoadBSEStockDailyHistoricalData for each stock in List_Of_Security_Codes
    for stock in List_Of_Security_Codes:
        try:
            SC=getCorporateActionsBSE(str(stock),Day_From,Month_From,Year_From, 
                                                Day_To,Month_To, Year_To,FD, npstocks)
            print(stock,SC)
            if SC==None:
                Stocks_failed_to_be_downloaded.append(stock)
        except() as e:
            print("Exception ", stock,e)
    print('SFTBD ',Stocks_failed_to_be_downloaded)
    print(npstocks)
    return npstocks


# In[44]:


def getCorporateActionsBSE(Security_Code,Day_From=1,Month_From='Jan',Year_From=1950, 
                                        Day_To=datetime.strftime(date.today(), '%d'),
                                        Month_To=datetime.strftime(date.today(), '%b'),
                                        Year_To=datetime.strftime(date.today(), '%Y'), 
                                        FD=str(os.path.expanduser('~')),invalidstocks=None):
    #Initializing driver
    driver=setWebDriver(FD+'CorporateActions/'+str(Security_Code)+'/',BSESCAURL)
    Year_From=int(Year_From)
    Year_To=int(Year_To)
    #Select 'Ex Date' for Select Date
    if selectPurpose(driver,'Ex Date','ContentPlaceHolder1_ddlcategory') is False:
       driver.quit()
       return
    if selectFromDate(driver,'ContentPlaceHolder1_txtDate',Day_From,Month_From,Year_From) is False:
        driver.quit()
        return
    if selectToDate(driver,'ContentPlaceHolder1_txtTodate',Day_To,Month_To,Year_To) is False:
        driver.quit()
        return
    stock=sendSecurityCode(driver, Security_Code,'ContentPlaceHolder1_SmartSearch_smartSearch')
    if stock is False:
        driver.quit()
        return 
    if (Security_Code != stock[0].text.split(' ')[-1]):
        invalidstocks.append(Security_Code)
        #print(Security_Code, 'not exist')
        driver.quit()
        return Security_Code
        #Select and Click From date box
    try:
       stock[0].click()
    except:
        driver.quit()
        return
    # Select 'Select' for action Purpose
    if selectPurpose(driver,'Select','ContentPlaceHolder1_ddlPurpose') is False:
        driver.quit()
        return
    
    if clickSubmitButton(driver,'ContentPlaceHolder1_btnSubmit') is False:
        driver.quit()
        return
    if os.path.isfile(FD+'CorporateActions/'+str(Security_Code)+'/Corporate_Actions.csv'):
        os.remove(FD+'CorporateActions/'+str(Security_Code)+'/Corporate_Actions.csv')
    if not os.path.isdir(FD+'CorporateActions/'+str(Security_Code)+'/'):
        try:
            os.makedirs(FD+'CorporateActions/'+str(Security_Code)+'/')
        except OSError:  
            print ("Creation of the directory %s failed" % FD+'CorporateActions/'+str(Security_Code)+'/')
    try:
        result=driver.find_element_by_xpath('//*[@id="ContentPlaceHolder1_lblData"]')
        if 'No Corporate Actions During Selected Period' in result.text:
            print('No Corporate Actions During Selected Period', Security_Code)
            invalidstocks.append(Security_Code)
            #os.rmdir(FD+'CorporateActions/'+str(Security_Code)+'/')
            driver.quit()
            return Security_Code
        elif clickDownloadButton(driver,'ContentPlaceHolder1_lnkDownload1') is False:
            driver.quit()
            return
    except:
        if clickDownloadButton(driver,'ContentPlaceHolder1_lnkDownload1') is False:
            driver.quit()
            return
    for i in range(20):
        if not os.path.isfile(FD+'CorporateActions/'+str(Security_Code)+'/Corporate_Actions.csv'):
            time.sleep(0.5)
        else:
            driver.quit()
            return Security_Code
            break 
    driver.quit()
    return


# In[46]:


def selectPurpose(driver,purpose,link):
    try:
        #Click Select Date Box for Ex Date
        wait = WebDriverWait(driver, 10)
        whichpurpose=wait.until(EC.element_to_be_clickable((By.ID, link)))
        selectpurpose=Select(whichpurpose)
        selectpurpose.select_by_visible_text(purpose) 
        return True
    except:
        return False


# In[47]:


if __name__ == '__main__':
    FD='/home/somesh/MSIT/MSIT 2nd Year/Practicum/'
    #FD='/home/hadoop/Documents/EquityAnalysis/Parallel/'
    CheckForNecessaryDirectories(FD)
    pc=mp.cpu_count()
#     downloadBSEActiveStocksList(FD)
    stocks = GetValidBSEActiveStocksListDF(FD)
#     print(len(stocks))
#     print("Valid Stocks: ",stocks)
#     DownLoadListOfBSEStocksDailyHistoricalData(None,1,'Jan',1950,
#                                            datetime.strftime(date.today(), '%d'),
#                                            datetime.strftime(date.today(), '%b'),
#                                            datetime.strftime(date.today(), '%Y'),28,FD,list())
    
#     runParallelSHDownload(FD,pc,stocks)
    stocks=getListOfStocksAlreadyDownloaded(FD+'BSEStocksDailyHistory/')
    npstocks=list()
    if os.path.isfile(FD+'ActiveStocks/CorporateActions/NPScrips.csv'):
        with open(FD+'ActiveStocks/CorporateActions/NPScrips.csv') as f:
            for line in f:
                npstocks.append(int(line.strip()))
    alreadydownLoadedstocks=getListOfStocksForCorporateActionsAlreadyDownloaded(FD+'CorporateActions/')
    [npstocks.append(elt) for elt in  alreadydownLoadedstocks]
    stocks=[stock for stock in stocks if stock not in npstocks]
    prevLenStocks=len(stocks)
#     print("Problem point alreadydownLoadedstocks null ",alreadydownLoadedstocks,len(stocks))
    while len(stocks)>0:
       prevLenStocks=len(stocks)
       runParallelCADownload(FD,pc,stocks)
       npstocks=getListOfStocksAlreadyDownloaded(FD+'CorporateActions/')
       #Add not processable security codes to remove from Active security codes
       alreadydownLoadedstocks=getListOfStocksForCorporateActionsAlreadyDownloaded(FD+'CorporateActions/')
       [npstocks.append(elt) for elt in  alreadydownLoadedstocks]
       stocks=[stock for stock in stocks if stock not in npstocks]
       if prevLenStocks==len(stocks):
           print(stocks)
           break


# In[ ]:


