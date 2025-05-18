import requests
from bs4 import BeautifulSoup
import csv
import os
import time
import json
import re

class Gsmarena():

    # Constructor to initialize common useful varibales throughout the program.
    def __init__(self):
        self.phones = []
        self.features = ["Brand", "Model Name", "Model Image"] # Initial base features
        self.temp1 = []
        self.phones_brands = []
        self.url = 'https://www.gsmarena.com/' # GSMArena website url
        self.new_folder_name = 'GSMArena_Dataset_Output' # Folder name on which files going to save, within ML_operations
        # Ensure this path is correct if running from ML_operations directory
        self.output_dir = os.path.join(os.getcwd(), self.new_folder_name)

    # This function crawl the html code of the requested URL.
    def crawl_html_page(self, sub_url):
        full_url = self.url + sub_url  
        header = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"} # Example User-Agent
        print(f"Requesting: {full_url}")
        time.sleep(10)  
        
        try:
            page = requests.get(full_url, timeout=15, headers=header)
            page.raise_for_status() # Raise an exception for HTTP errors (4xx or 5xx)
            soup = BeautifulSoup(page.text, 'html.parser')  # It parses the html data from requested url.
            return soup
        except requests.exceptions.ConnectionError as err:
            print(f"Connection error for {full_url}: {err}")
            return None # Allow graceful continuation or exit
        except requests.exceptions.Timeout as err:
            print(f"Timeout for {full_url}: {err}")
            return None
        except requests.exceptions.HTTPError as err:
            print(f"HTTP error for {full_url}: {err}")
            return None
        except Exception as e:
            print(f"An unexpected error occurred while fetching {full_url}: {e}")
            return None

    # This function crawl mobile phones brands and return the list of the brands.
    def crawl_phone_brands(self):
        phones_brands_list = []
        soup = self.crawl_html_page('makers.php3')
        if not soup:
            print("Could not fetch phone brands page. Exiting.")
            return []
        table = soup.find('table') # First table should be the brands table
        if not table:
            print("Could not find brands table on makers.php3. Exiting.")
            return []
        
        table_a = table.find_all('a')
        for a in table_a:
            # Example: <a href="apple-phones-48.php"><span>APPLE</span><br><i>117 devices</i></a>
            if a.find('span'): # Ensure span exists
                brand_name_full = a.find('span').text.strip() # e.g., APPLE
                href = a['href'] # e.g., apple-phones-48.php
                # Extracting a simpler brand name if possible, otherwise use full
                simple_brand_name = brand_name_full.split(' ')[0] 
                temp = [simple_brand_name, brand_name_full, href]
                phones_brands_list.append(temp)
            else:
                print(f"Skipping an entry in brand list, no span found: {a}")
        return phones_brands_list

    # This function crawl mobile phones brands models links and return the list of the links.
    def crawl_phones_models(self, phone_brand_link):
        links = []
        all_page_links_for_brand = [] # Store all page links (e.g., page 1, page 2 of a brand)
        
        print(f"Fetching initial models page: {phone_brand_link}")
        soup = self.crawl_html_page(phone_brand_link)
        if not soup:
            return []

        nav_data = soup.find(class_='nav-pages')
        if not nav_data:
            all_page_links_for_brand.append(phone_brand_link) # Only one page of models
        else:
            all_page_links_for_brand.append(phone_brand_link) # Add the first page itself
            for page_link_tag in nav_data.findAll('a'):
                all_page_links_for_brand.append(page_link_tag['href'])
            # Remove duplicates if any, though order might change slightly
            all_page_links_for_brand = sorted(list(set(all_page_links_for_brand)))

        print(f"Found {len(all_page_links_for_brand)} pages of models for this brand.")

        for page_url in all_page_links_for_brand:
            print(f"Fetching models from page: {page_url}")
            soup_page = self.crawl_html_page(page_url)
            if not soup_page:
                continue # Skip this page if fetching failed
            
            data_container = soup_page.find(class_='makers') # Usually models are within <div class="makers"><ul><li>...
            if not data_container:
                data_container = soup_page.find(class_='section-body') # Fallback for some layouts
            
            if data_container:
                for line1 in data_container.findAll('a'):
                    links.append(line1['href'])
            else:
                print(f"Could not find model links container on {page_url}")
        
        return list(set(links)) # Return unique model links

    # This function crawl mobile phones specification and return the list of the all devices list of single brand.
    def crawl_phones_models_specification(self, model_page_link, phone_brand_simple_name):
        phone_data = {}
        print(f"Fetching specs for model: {model_page_link}")
        soup = self.crawl_html_page(model_page_link)
        if not soup:
            return None

        model_name_tag = soup.find(class_='specs-phone-name-title')
        model_name = model_name_tag.text.strip() if model_name_tag else "Unknown Model"
        
        model_img_tag = soup.find(class_='specs-photo-main')
        model_img = model_img_tag.find('img')['src'] if model_img_tag and model_img_tag.find('img') else "No Image"
        
        phone_data["Brand"] = phone_brand_simple_name
        phone_data["Model Name"] = model_name
        phone_data["Model Image"] = model_img
        
        # Dynamically build self.features here or ensure it's comprehensive enough before writing CSV headers
        # For now, self.features is added to globally by the caller before writing CSV if new keys appear.

        spec_tables = soup.findAll('table')
        for table in spec_tables:
            rows = table.findAll('tr')
            for row in rows:
                cells = row.findAll('td')
                if len(cells) == 2: # Expecting key-value pairs
                    feature_name_raw = cells[0].getText().strip().replace("\n", " ")
                    feature_value_raw = cells[1].getText().strip().replace("\n", " ")
                    
                    # Clean up potential multiple spaces
                    feature_name = re.sub(r'\s+', ' ', feature_name_raw)
                    feature_value = re.sub(r'\s+', ' ', feature_value_raw)

                    # Handle duplicate feature names by appending '_' + count (more robust than just '_1')
                    original_feature_name = feature_name
                    count = 1
                    while feature_name in phone_data:
                        feature_name = f"{original_feature_name}_{count}"
                        count += 1
                    
                    phone_data[feature_name] = feature_value
                    if feature_name not in self.features:
                         self.features.append(feature_name) # Keep track of all unique feature names found
        return phone_data

    # This function create the folder.
    def create_output_folder(self):
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
            print(f"Creating {self.output_dir} Folder....")
            time.sleep(1) # Shorter sleep
            print("Folder Created.")
        else:
            print(f"{self.output_dir} directory already exists")

    # This function save the devices specification to csv file.
    def save_specification_to_file(self):
        MAX_BRANDS_TO_SCRAPE = 1 # Set to 1 or 2 for testing, or a larger number / None to scrape all
        brands_scraped_count = 0

        all_phone_brands = self.crawl_phone_brands()
        if not all_phone_brands:
            print("No brands found. Exiting scraper.")
            return
            
        self.create_output_folder()
        
        for brand_info in all_phone_brands:
            if MAX_BRANDS_TO_SCRAPE is not None and brands_scraped_count >= MAX_BRANDS_TO_SCRAPE:
                print(f"Reached MAX_BRANDS_TO_SCRAPE limit of {MAX_BRANDS_TO_SCRAPE}. Stopping.")
                break

            simple_brand_name, full_brand_name, brand_page_link = brand_info
            csv_file_name = os.path.join(self.output_dir, simple_brand_name.replace('/', '_') + '.csv') # Sanitize filename
            
            # Check if file already exists to potentially skip (optional)
            # if os.path.exists(csv_file_name):
            #     print(f"{csv_file_name} already exists. Skipping {simple_brand_name}.")
            #     brands_scraped_count += 1
            #     continue

            print(f"--- Working on {full_brand_name} brand --- ")
            model_page_links = self.crawl_phones_models(brand_page_link)
            if not model_page_links:
                print(f"No models found for {full_brand_name}. Skipping.")
                continue
            
            brand_all_phones_data = []
            models_processed_count = 0
            print(f"Found {len(model_page_links)} models for {full_brand_name}.")

            for model_link in model_page_links:
                datum = self.crawl_phones_models_specification(model_link, simple_brand_name)
                if datum:
                    brand_all_phones_data.append(datum)
                models_processed_count += 1
                print(f"Processed model {models_processed_count}/{len(model_page_links)} for {full_brand_name}")
            
            if not brand_all_phones_data:
                print(f"No data collected for any models of {full_brand_name}. Skipping file write.")
                brands_scraped_count += 1
                continue
            
            # Ensure all features found across all phones for this brand are in self.features
            # (self.features is updated dynamically in crawl_phones_models_specification)
            # For DictWriter, it's best if fieldnames are known upfront or consistent.
            # Here, self.features grows dynamically. This is okay if we write one CSV per brand and then merge,
            # or if we collect ALL features from ALL brands first (more complex).
            # The current approach of one CSV per brand using a dynamically growing self.features is a bit problematic
            # if you want all CSVs to have exactly the same columns unless self.features is global and finalized *before* writing any CSV.
            # For robust CSV writing, it might be better to collect all_features from all phones of a brand first.
            
            current_brand_features = list(self.features) # Use a snapshot of features known up to this brand
            # Or, more robustly, get all keys from all dicts in brand_all_phones_data:
            # all_keys_for_brand = set()
            # for phone_dict in brand_all_phones_data:
            #    all_keys_for_brand.update(phone_dict.keys())
            # current_brand_features = sorted(list(all_keys_for_brand))
            # if not current_brand_features: current_brand_features = self.features # Fallback
            
            # Using the global self.features which accumulates over time.
            # This means later brands might have more columns in their headers if new features are found.
            # For consistency, it's better to pre-scan or fix a feature set.
            # However, for simplicity of the provided code, sticking to self.features.

            print(f"Writing {len(brand_all_phones_data)} phones for {full_brand_name} to {csv_file_name} using {len(self.features)} features.")
            try:
                with open(csv_file_name, "w", newline='', encoding='utf-8') as file:
                    # Use a comprehensive field list from all data collected for this brand
                    fieldnames_for_this_brand = set()
                    for row_data in brand_all_phones_data:
                        fieldnames_for_this_brand.update(row_data.keys())
                    final_fieldnames = sorted(list(fieldnames_for_this_brand))
                    if not final_fieldnames : final_fieldnames = self.features # fallback if no data

                    dict_writer = csv.DictWriter(file, fieldnames=final_fieldnames, extrasaction='ignore')
                    dict_writer.writeheader()
                    for phone_dict in brand_all_phones_data:
                        dict_writer.writerow(phone_dict)
                print(f"Data for {full_brand_name} saved to {csv_file_name}")
            except Exception as e:
                print(f"Error writing CSV for {full_brand_name}: {e}")

            brands_scraped_count += 1
            print(f"--- Finished {full_brand_name} brand --- \n")

if __name__ == "__main__":
    scraper = Gsmarena()
    try:
        scraper.save_specification_to_file()
    except KeyboardInterrupt:
        print("Scraping stopped due to Keyboard Interrupt.")
    except Exception as e:
        print(f"An unexpected error occurred during scraping: {e}") 