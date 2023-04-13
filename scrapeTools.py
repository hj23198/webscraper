import bs4
from bs4 import BeautifulSoup
import os
import pandas as pds
import json
import random
import time
import logging
logging.basicConfig(filename="output.log", level="INFO") 


class tableScraper():
    """Requires child class. Structure: \n
    __init__(database_path, webdriver):
        Path for database to be stored in, webdriver to scrape with

    __createStatus__():
        Returns list of urls to be scraped

    __pageActions__(webdriver):
        Called upon when visiting a page for the first time. Schedules actions for scraping by returning a list of parameters.

    __onLoad__(webdriver, options):
        Allows for interations with the page before the content is scraped. Options is a value in the list returned by __pageActions__():

        Ex. Imagine there is a webpage with a table on it. A dropdown allows for different table content to be displayed. The goal is to scrape all of the different tables. 
        
        The call to __pageActions__() will return
        a list of the different dropdown options. The call to __onLoad__() will take in each of the dropdown options and select the element before scraping the page.  
        """
    def __init__(self, loaderClass, webdriver, database_path) -> None:
        """Prepares database structure and loads in urls to be visited"""
        self.database_path = database_path
        self.webdriver = webdriver
        """Selenium webdriver to scrape with"""
        self.loaderClass = loaderClass
        """Child class described by tableScraper doc"""

        self.toVisit = []
        """List of queued urls to visit. Functions as a FILO stack."""
        self.database = {}
        """Contains scraped data. Keys are table names, values are dataframes of table content."""
        self.config = {
            "saveInterval":10
        }
        """saveInterval: Amount of successful page visits between saving data"""

        self.__createDatabaseStructure__()
        self.__loadStatus__()
        self.__loadDatabase__()

        
    def start(self):
        """Starts the scraper"""

        visited_count = 0
        while len(self.toVisit) != 0:

            if visited_count != 0 and visited_count % self.config["saveInterval"] == 0:
                self.__saveStatus__()
                self.__saveDatabase__()

            time.sleep(2 + random.random())
            url = self.toVisit.pop()

            logging.info(f"Visiting {url}")

            try:
                self.webdriver.get(url)
            except Exception as ex:
                self.toVisit.insert(0, url)
                logging.warning(f'Encountered exception when visiting "{url}":\n {ex}')
                continue
            
            if hasattr(self.loaderClass, "__pageActions__"):
                try:
                    action_list = self.loaderClass.__pageActions__(self.webdriver)
                except Exception as ex:
                    self.toVisit.insert(0, url)
                    logging.error(f'Encountered exception when calling __pageActions__() on page "{url}":\n {ex}')
                    continue
            else:
                extracted_content = self.__extractTables__(self.webdriver.page_source, [[]])
                self.__combineTables__(extracted_content)
                visited_count += 1
                continue

            
            extracted_content = []
            continue_var = False
            for action in action_list:
                try:
                    label_columns = self.loaderClass.__onLoad__(self.webdriver, action)
                except Exception as ex:
                    logging.error(f'Encountered exception while calling __onLoad__() on page "{url}", action "{action}:\n {ex}')
                    self.toVisit.insert(0, url)
                    continue_var = True
                    break

                extracted_content.append(self.__extractTables__(self.webdriver.page_source, label_columns))

            if continue_var:
                continue

            for content in extracted_content:
                self.__combineTables__(content)

            visited_count += 1

        self.__saveDatabase__()
        self.__saveStatus__()

    def __createDatabaseStructure__(self):
        """Populates any missing files in database"""
        if not os.path.isdir(self.database_path):
            os.mkdir(self.database_path)

        if not os.path.isdir(os.path.join(self.database_path, "data")):
            os.mkdir(os.path.join(self.database_path, "data"))

    def __saveDatabase__(self):
        """Saves data in self.database to database"""
        for fname in self.database:
            self.database[fname].to_csv(os.path.join(self.database_path, "data", f"{fname}.csv"))

    def __loadDatabase__(self):
        """Loads data from database into self.database"""
        file_in_database = [f for f in os.listdir(os.path.join(self.database_path, "data")) if f[0] != '.']
        for file in file_in_database:
            #remove .csv from file name to extract table name
            key_name = file[0:len(file)-4]
            self.database[key_name] = pds.read_csv(os.path.join(self.database_path, "data", file))

    def __loadStatus__(self):
        """Loads urls to be scraped into list self.toVisit."""
        fpath = os.path.join(self.database_path, "status.json")
        if not os.path.isfile(fpath):
            self.toVisit = self.loaderClass.__createStatus__()
            with open(fpath, "w") as file:
                json.dump(self.toVisit, file)
        else:
            with open(fpath, "r") as file:
                self.toVisit = json.load(file)

    def __saveStatus__(self):
        """Updates status.json with self.toVisit"""
        with open(os.path.join(self.database_path, "status.json"), "w") as file:
            json.dump(self.toVisit, file)

    def __combineTables__(self, listToCombine):
        """Combines list of dataframes into self.database where columns are equal to eachother. 
        If there are no matches, append to self.database instead."""
        for combineKey in listToCombine:
            if combineKey in self.database.keys():
                self.database[combineKey] = pds.concat((self.database[combineKey], listToCombine[combineKey]))          
            else:
                self.database[combineKey] = listToCombine[combineKey]

    def __extractTables__(self, html_content, added_rows):
        """Extrats dictionary of dataframes from tables in html_content. \n
           Keys are table titles, values are dataframes.\n

           Added rows are added to each column in the content, ex [[col, value], [col, value]]
           """

        tables = BeautifulSoup(html_content, features="html.parser").find_all("table")
        tables = [t for t in tables if len(t.find_all("thead")) > 0]
        extracted_tables = dict([self.__tableToDataframe__(table, added_rows) for table in tables])
        return extracted_tables

    def __tableToDataframe__(self, table: bs4.element.Tag, added_rows) -> pds.DataFrame:
        """Extracts BeautifulSoup table tag content into dataframe. \n
        Returns table_title, dataframe \n
        On error, returns String with error message instead.
        
        Added rows injects list of [row, column] pairs into each table"""
        header = table.find_all("thead")[0]
        head_rows = header.find_all("tr")

        if len(head_rows) != 2:
            return f"Expected to find 2 'tr' in 'thead'. Instead found {len(head_rows)}."

        head_title = head_rows[0].text
        head_title = self.__cleanTableName__(head_title)
        head_row = head_rows[1]

        head_cat = [i.text.replace("\xa0", " ").replace("\n", " ") for i in head_row.find_all("th")]

        #If the table is unlabeled and returns no columns (resulting in len() == 0), instead we count the number of entries in earch row 
        if len(head_cat) == 0:
            head_cat_len = len(table.find_all("tbody")[0].find_all("tr")[0])
            head_cat = [f"Column {i}" for i in range(head_cat_len)]

        if added_rows != [[]]:
            for added_col in added_rows:
                head_cat.append(added_col[0])

        table_content = pds.DataFrame(columns=head_cat)

        body = table.find_all("tbody")[0]
        rows = body.find_all("tr")
        for row in rows:

            row_to_add = [i.text for i in row.find_all("td")]
            if added_rows != [[]]:
                for item in added_rows:
                    row_to_add.append(item[1])

            row_to_add = dict(zip(head_cat, row_to_add))

            if added_rows != [[]]:
                for added_row in added_rows:
                    row_to_add[added_row[1]] = added_row[0]

            table_content.loc[len(table_content.index)] = row_to_add

        return head_title, table_content

    def __cleanTableName__(self, name):
        """Cleans characters from table name so it can be used as a file name"""
        to_clean = ['<', '>', ':', '"', "/", "\\", '?', '|', "*", "{", "}", "[", "]", "-"]
        for i in to_clean:
            name = name.replace(i, "")
        return name

