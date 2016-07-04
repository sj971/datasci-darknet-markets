#!/usr/bin/env python2
import os
from os import listdir, path
from bs4 import BeautifulSoup
import pandas as pd
import re

# set path to folders containing local project code and output files
project_path = "/Users/Stuart/Documents/DATASCIENCE/Insight/project"
code_path = project_path + "/code"
output_path = project_path + "/output/evolution"

# set path to raw data archive (see readme.txt)
raw_data_path = "/Volumes/INSIGHT/evolution"

# get directory tree of archive (should be ~110 dates in total)
dates = os.listdir(raw_data_path)

# set id counter
id_counter = 0

# prepare regex pattern for removing junk symbols from product info (keeping $, %, decimal point, etc.)
pattern = re.compile('[^\w.$%-/\() ]') # helps avoid MySQL read in problems

# go through archive date-by-date
for date in range(1, len(dates)): #check: index from 1 if first sub-directory == .DS_Store

    # prepare relevant path/listing info.
    which_date = raw_data_path + "/" + dates[date] + "/listing"
    listings_this_date = os.listdir(which_date)
    listings_to_dataframe = pd.DataFrame()
    
    # print which date in the archive is currently being processed
    print which_date
    
    # go through this date listing-by-listing
    for listing in range(0, len(listings_this_date)):
        
        # ignore pages that appear to be partial duplicates of others
        if '.' in listings_this_date[listing]:
        
            print 'Possible duplicate page found: Ignore'
            
        else:
            
            # set current path
            current_path = which_date + "/" + listings_this_date[listing]

            try:

                # is this a file or directory?
                short_path = []
                if os.path.isfile(current_path): # single file (i.e., feedback comments don't extend into second or more pages)

                    # open this html page and store archive path for database
                    current_listing = open(current_path)
                    short_path = "evolution/" + dates[date] + "/listing/" + listings_this_date[listing]

                else: # directory (i.e., feedback comments extend into second or more pages; we're only interested in first page)

                    # open relevant html page from subdirectory and store archive path for database
                    current_listing = open(current_path + "/feedback")
                    short_path = "evolution/" + dates[date] + "/listing/" + listings_this_date[listing] + "/feedback"


                # is this a page with a drug listing?  
                soup = BeautifulSoup(current_listing, 'html.parser')         
                drug_listing = soup.find_all("li", string = "Drugs")

                # if this is a page with a drug listing, extract relevant info.
                if drug_listing:

                    # update id counter
                    id_counter += 1 

                    # create empty lists
                    product = []
                    origin = []
                    vendor = []
                    price = []

                    # note: the html tagging format for several of the variables (e.g., origin, vendor) changed part of the way through the archive timeline; 
                    # I forgot to note the exact date(s) of format change, but this could be checked by commenting out one of the alternative code snippets, and 
                    # seeing where the code breaks. The code below was initially developed before processing the Agora dataset; as with Agora, the code is by necessity 
                    # somewhat ad-hoc in nature, very specific to the Evolution website's html tags and format, and likely not optimal. However, it runs reasonably 
                    # robustly across the full dataset, with a few exceptions which are handled below in slightly hacky ways and were developed through trial-and-error.
                    # Technically, some of the if/elif statements below are not guaranteed opposites (i.e., not guaranteed to exclude the alternative); however, running 
                    # the code raised no problems, and my initial trial-and-error debugging and visual inspection of random html files led me to the conclusion that the 
                    # differences are exclusive (i.e., stem from comprehensive html tag reformatting at particular timepoints). This part of the code, however, could 
                    # likely be improved in formal precision.

                    # product
                    if len(soup.title.string) > 0:

                        product = soup.title.string                   
                        product = product.replace("\r", " ")
                        product = product.replace("\n", " ")
                        product = product.replace(",", "")
                        product = product.replace("*", "")
                        if 'Evolution' in product:

                            product = product.replace('Evolution', '')

                        if 'Listing' in product:

                            product = product.replace('Listing', '')

                        product = re.sub(pattern, '', product)
                        product = product.strip()

                    else:

                        product = 'NotFound'


                    # origin
                    if len(soup.find_all("div", "widget")) > 1: # relevant tag for one part of timeline

                        temp = soup.find_all("div", "widget")[1]
                        if 'required' in str.lower(str(temp.find_all("p")[0].string)): 

                            temp = soup.find_all("div", "widget")[2]
                            origin = temp.find_all("p")[0].string

                        else:

                            origin = temp.find_all("p")[0].string

                        origin = origin.replace("\r", " ")
                        origin = origin.replace("\n", " ")
                        origin = origin.replace(",", "")
                        origin = origin.strip()

                    elif len(soup.find_all("dd")) > 2: # relevant tag for a different part of timeline

                        origin = soup.find_all("dd")[2].string             
                        origin = origin.replace("\r", " ")
                        origin = origin.replace("\n", " ")
                        origin = origin.replace(",", "")
                        origin = origin.strip()

                    else:

                        origin = 'NotFound'


                    # vendor
                    if len(soup.find_all("div", "seller-info text-muted")) > 0: # relevant tag for one part of timeline

                        temp = soup.find_all("div", "seller-info text-muted")[0]
                        vendor = temp.find_all("a")[0].string
                        vendor = vendor.strip()

                    elif len(soup.find_all("p")) > 1: # relevant tag for a different part of timeline

                        temp = soup.find_all("p")[1]
                        vendor = temp.find_all("a")[0].string
                        vendor = vendor.strip()

                    else:

                        vendor = 'NotFound'


                    # price
                    if len(soup.find_all("h4")) > 0:

                        price = soup.find_all("h4")[0].string
                        price = price.strip()

                    else:

                        price = 'NotFound'


                    # convert listing info. to pandas series
                    series_id = pd.Series(id_counter)
                    series_date = pd.Series(dates[date])
                    series_path = pd.Series(short_path)             
                    series_product = pd.Series(product)
                    series_origin = pd.Series(origin)
                    series_vendor = pd.Series(vendor)
                    series_price = pd.Series(price)

                    # append listing info to pandas dataframe
                    listings_to_dataframe = listings_to_dataframe.append(pd.concat([series_id, series_date, series_path, series_product, series_origin, series_vendor, series_price], axis = 1), ignore_index = 'TRUE')

                # close current listing
                current_listing.close()

            except IOError:

                 pass
  
    # if drug listings found for this date, save to .csv
    if listings_to_dataframe.empty:
        
        print 'No drug listings found for this date'
    
    else:

        # add column names to dataframe
        listings_to_dataframe.columns = ['id', 'date', 'path', 'product', 'origin', 'vendor', 'price']

        # save dataframe for this date to .csv
        save_to_filename = output_path + "/" + dates[date] + "_evolution.csv"
        listings_to_dataframe.to_csv(save_to_filename, encoding = 'utf-8', index = False)
    