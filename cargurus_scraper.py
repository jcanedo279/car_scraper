from bs4 import BeautifulSoup
import pandas as pd
import time
import sys
import os

from util_scraper import *



###################
### Preferences ###
###################

find_last_page = True
starting_page = 1

BUFFER_LINE = ' '*5


clean_data = "data/_{sns.car_name}_clean.csv"



########################
### Grab cached data ###
########################

car_dir = f'data_{sns.car_name}/'
if not os.path.exists(car_dir):
    os.mkdir(car_dir)

known_cars = set()
file_path = os.path.abspath(__file__)

print("Started scraping local datasets\r", end="")
if find_last_page:
    max_page = 0
    
    for _file in os.listdir( '/'.join( file_path.split('/')[:-1] ) + f'/data_{sns.car_name}' ):
        
        ## Find max page
        page_ind = _file.split('_')[-1].replace('.csv','')
        max_page = max(int(page_ind), max_page)
        
        ## Add references
        df_i = pd.read_csv(f'data_{sns.car_name}/{_file}')
        for _, row_i in df_i.iterrows():
            ref_i = (row_i['car_title'], row_i['price'], row_i['mileage'])
            known_cars.add(ref_i)

    starting_page = max_page + 1
print(" "*100+"\r", end="")




link = f"https://www.cargurus.com/Cars/inventorylisting/viewDetailsFilterViewInventoryListing.action?zip={sns.zipcode}&showNegotiable=true&sortDir={sns.search_mode[0]}&sourceContext=carGurusHomePage_false_0&distance={sns.distance}&sortType={sns.search_mode[1]}&entitySelectingHelper.selectedEntity={sns.vehicle_code}#resultsPage={starting_page}"





total_page_time = 0
total_car_time = 0
total_cars = 0

with DriverHandler(link) as driver:
    
    print(f"\n\nExtracting {sns.num_pages} pages ({starting_page}->{starting_page+sns.num_pages}) from:   {link[:30]}...{link[-30:]}\n")

    assert "CarGurus" in driver.title


    for i in range(sns.num_pages):
        
        page_start_time = time.time()
        
        data = []
        

        html = driver.page_source
        soup = BeautifulSoup(html, "html.parser")
        
        cars = soup.find_all("div", {"data-cg-ft":"car-blade"})
        num_cars = len(cars)
        
        
        avg_time_page = 'N/A' if not total_page_time else total_page_time/(i+1)

        
        for car_ind, car in enumerate(cars):
            car_start_time = time.time()
            avg_time_cars = 'N/A' if not total_car_time else total_car_time/(total_cars+car_ind)
            
            print(f"Scraping page #{i+1}: [{starting_page+i} | {starting_page+sns.num_pages}]{BUFFER_LINE}Scraping car: [{car_ind+1} | {num_cars}]{BUFFER_LINE}Average page time: {avg_time_page}{BUFFER_LINE}Average car time: {avg_time_cars}{BUFFER_LINE}\r", end='')
            
            row = {}
            
            title = car.find_all("div", {"data-cg-ft":"srp-listing-blade-title"})[0].text
            
            row['car_title'] = title
            
            ####### FOLLOW CAR #########
            
            car_elements = driver.find_elements_by_xpath("//a[@data-cg-ft='car-blade-link']")
            driver.execute_script("arguments[0].click()", car_elements[car_ind])
            time.sleep(CNST_SLEEP)
            
            car_html = driver.page_source
            car_soup = BeautifulSoup(car_html, "html.parser")
            
            drive = driver.find_elements_by_xpath("//div[@data-cg-ft='vdp-listing-navigation']")
            
            
            ######################
            ### PRICE ANALYSIS ###
            ######################
            mrk_anal_details = drive[0].find_elements_by_xpath("//section[@data-cg-ft='vdp-deal-rating']")
            delta_price = 0
            if mrk_anal_details:
                mrk_anal_info = mrk_anal_details[0].text.split('\n')
                if len(mrk_anal_info) >= 3:
                    mrk_anal_dir, mrk_anal_price = mrk_anal_info[1], mrk_anal_info[2]
                    if '$' == mrk_anal_price[0]:
                        mrk_anal_dir = 1 if 'below' in mrk_anal_dir.lower() else -1
                        mrk_anal_price = mrk_anal_price[1:].replace(',','')
                        mrk_anal_price = 0 if 'x' in mrk_anal_price.lower() else int(mrk_anal_price)
                        delta_price = mrk_anal_dir * mrk_anal_price
            
            
            ####################
            ### LISTING DATA ###
            ####################
            listing_details = drive[0].find_elements_by_xpath("//section[@data-cg-ft='listing-details']")
            if not listing_details:
                skip_datapoint(driver)
                continue
            listing_info = listing_details[0].text.split("\n")
            
            
            attributes = [('price', ["Dealer's Price:", "Seller's Price:"]), ('mileage', ["Mileage:"]), ('transmission', ["Transmission:"]), ('ext_color', ["Exterior Color:"]), ('int_color', ["Interior Color:"]), ('drivetrain', ["Drivetrain:"])]
            
            # print(listing_info)
            options = None
            try:
                options_ind = listing_info.index('Major Options:')
                options_parse = listing_info[options_ind+1:]
                options = []
                for item in options_parse:
                    if ',' in item: options.extend([val.strip() for val in item.split(',')])
                    else: options.append(item)
                options = set(options)
            except ValueError:
                options = set()
            # print(options)


            tag_to_option = [('SP', 'Sound Package'), ('LS', 'Leather Seats'), ('AW', 'Alloy Wheels'), ('CP', 'CarPlay'), ('HS', 'Heated Seats'), ('AS', 'Adaptive Suspension'), ('BC', 'Backup Camera'), ('S_M', 'Sunroof/Moonroof'), ('SCP', 'Sport Chrono Package'), ('BHE', 'Bose High End Sound Package'), ('BXH', 'Bi Xenon Headlamp Package'), ('PP', 'Premium Package'), ('MP', 'Memory Package'), ('SP', 'Sport Package'), ('CP', 'Comfort Package'), ]
            
            for tag,option in tag_to_option:
                row[tag] = 1 if option in options else 0
            
            row['certified'] = int('Certified:' in options and 'Yes' in options)
            row['num_options'] = sum([row[tag] for tag,_ in tag_to_option])
            
            missing_data = False
            for att, qualifiers in attributes:
                for qualifier in qualifiers:
                    if not qualifier in listing_info: continue
                    row[att] = listing_info[listing_info.index(qualifier)+1]
                if att not in row:
                    missing_data = True
                    break
                if row[att] == '---':
                    missing_data = True
                    break
            if missing_data:
                skip_datapoint(driver)
                continue
            
            
            ## Remove comments from numericals
            classes = ['price', 'mileage']
            for _class in classes:
                if ' ' in row[_class]:
                    row[_class] = row[_class][:row[_class].index(' ')]
            
            # print(row)
                
            ## Numericals -> Strings 
            row['price'] = int( row['price'][1:].replace(',','') )
            row['price'] += delta_price/2
            row['mileage'] = int( row['mileage'].replace(',','') )
            
            ## Remove comments from colors
            dels = ['(', '/']
            colors = ['ext_color', 'int_color']
            for color in colors:
                for _del in dels:
                    try:
                        if _del in row[color]:
                            row[color] = row[color][:row[color].index(_del)]
                    except KeyError:
                        print(color, _del)
                        print(row)
            for color in colors:
                row[color] = row[color].lower()
            
            
            
            
            #####################
            ### ACCIDENT DATA ###
            #####################
            accident_data = drive[0].find_elements_by_xpath("//div[@id='history']")
            if not accident_data:
                skip_datapoint(driver)
                continue
            
            accident_info = accident_data[0].text.split('\n')
            
            class_indexing = [('title',2),('accident',4),('owners',5)]
            missing_data = False
            for _class, index in class_indexing:
                try:
                    row[_class] = accident_info[index].lower()
                except ValueError:
                    missing_data = True
                    break
                except IndexError:
                    missing_data = True
                    break
            if missing_data:
                skip_datapoint(driver)
                continue
            
            ## Reformat accident information
            breakups = [('owners','own'), ('accident','accident')]
            for _class,_break in breakups:
                if _break in row[_class]:
                    row[_class] = row[_class][:row[_class].index(_break)]
            for _class in ['title','accident','owners']:
                if ' ' in row[_class]:
                    row[_class] = row[_class][:row[_class].index(' ')]
            
            ## Correct accident information
            corrections = [('accident','no','0'), ('accident','frame','10'), ('owners','one','1')]
            for _class, original, new in corrections:
                if row[_class] == original:
                    row[_class] = new
            
            skip_datapoint(driver)
            
            
            # mark_anal_data = drive[0].find_elements_by_xpath("//div[@class='OcTgyk']")
            # # if mark_anal_data:
            # print('nvm')
            # print(mark_anal_data)
                
            # mark_anal_info = mark_anal_data[0].text
            # print(mark_anal_info)
            
            
            
            ref = (title, row['price'], row['mileage'])
            
            if ref not in known_cars:
                known_cars.add(ref)
                data.append(row)
        
            total_car_time += time.time() - car_start_time
        
        total_cars += num_cars
        
        # next_page = driver.find_element_by_class_name("xkKA_W")
        try:
            next_page = driver.find_element_by_xpath("//button[@data-cg-ft='page-navigation-next-page']")
        except:
            push_data(data, starting_page+i)
            print('Finished scraping CarGurus')
            sys.exit(0)
        
        driver.execute_script("arguments[0].click();", next_page)
        time.sleep(CNST_SLEEP)
        
        if not data:
            print('Data incongruency, no data found!')
            sys.exit(0)
        
        push_data(data, starting_page+i)
        
        total_page_time += time.time() - page_start_time
        




