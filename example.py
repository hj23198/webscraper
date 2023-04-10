from scrapeTools import tableScraper

from selenium import webdriver
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
import time

class scraper(tableScraper):
    def __init__(self, webdriver, database_path) -> None:
        super().__init__(self, webdriver, database_path)

    def __createStatus__(self):
        #Return a list of the urls to be scraped
        #If the scraper is restarted, database_path/status.json will be used instead
        # database_path/status.json keeps track of the urls that have not been visited yet 
        #If exceptions occur, the pages data will not be saved and it is sent to the back of the queue
        return ["https://www.nba.com/stats/team/1610612737", "https://www.nba.com/stats/team/1610612743", "https://www.nba.com/stats/team/1610612738"]

    def __pageActions__(self, webdriver):
        #This function creates a list of actions when first visiting a specific url
        #The list of actions are then iteritivly passed into __onLoad__()
        #If you do not need to interact with a webpage, do not define this function 

        #get page source from webdriver
        page_text = webdriver.page_source
        #parse out list of options from a dropdown menu
        soup = BeautifulSoup(page_text, features="html.parser")
        options = [i.text for i in soup.find_all("select")[0].children]
        return options

    def __onLoad__(self, webdriver, option):
        time.sleep(2)
        #Now, the input of option will be one element of the list of options returned by calling __pageActions__() on the page
        #In this case, we have a year in a dropdown menu, which is selected
        #After this funtion ends, the page is scraped for its table contents. 

        #find dropdown element and select 
        element = Select(webdriver.find_element(By.XPATH, '//*[@id="__next"]/div[2]/div[2]/main/div[3]/section[2]/div/div[1]/div/label/div/select'))
        element.select_by_visible_text(option)

        #returned [col, row] sets will be added to each table entry in the webpage 
        #return [[]] to add no extra cols 
        return [["randomcol", "randomrow"], ["anotherrandomcol", "anotherrandomrow"]]

#I used firefox while creating this, no idea if other drivers work the same way         
driver = webdriver.Firefox()

#The file structure will be created automatically. All you need is an empty directory 
database_path = "path/to/database/dir"

loader = scraper(driver, database_path)

#save scraped content every 10 pages in case an exception is encountered
#this is the only option currently 
loader.config["saveInterval"] = 10

#starts scraper
loader.start()
