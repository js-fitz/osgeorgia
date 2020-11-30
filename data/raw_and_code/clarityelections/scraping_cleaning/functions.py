import os
import re
import time
import jaro
import config
import shutil
import zipfile
import pandas as pd
from tqdm.notebook import tqdm
from fuzzywuzzy import fuzz
from selenium import webdriver
from datetime import datetime as dt

# set download directory:
config.download_dir = '/Users/joe/Downloads'
try:
    assert os.path.isdir(config.download_dir)
    print(f"Download directory set to '{config.download_dir}'")
except: 
    print(f"Download directory NOT FOUND '{config.download_dir}'")
    print(f">>> Set the browser's default D/L path: 'config.download_dir = _________'\n")
    
max_down_wait_sec = 60 # scraper cancels after failing for this long


#### get all county result page URLs
def pre_scraper(state, election_id):
    url = f'https://results.enr.clarityelections.com/{state}/{election_id}/web.264614/#/access-to-races'
    d = webdriver.Chrome()
    d.get(url)
    
    print('\nELECTION DETAILS:')
    time.sleep(2) # wait for pageload. could be conditional...
    config.data_via = d.find_element_by_class_name('navbar-brand').text
    race_details = d.find_element_by_class_name('jumbotron').text.split('\n')
    config.race_date = race_details[0]
    config.race_type = race_details[1]
    print('   >', config.race_type, '-', config.race_date)
    print('   > source:', config.race_date)
    
    # find county completation rate stats
    county_stats = [c.text for c in
             d.find_element_by_class_name('sidebar'
             ).find_element_by_class_name('card-body'
             ).find_elements_by_tag_name('div')]
    county_stats = [c for c in county_stats if 'counties complete' in c.lower()][0]
    county_stats = county_stats.split(':')[-1].split('/')
    config.n_counties = int(county_stats[0])
    n_reporting = int(county_stats[1])
    print(f'   > {n_reporting} counties reporting ({config.n_counties} total)')    
    
    # collect all links on the page

    
    est_pg_height = int(config.n_counties/3)*50 + 500
    all_links = []
    for y in range(500, est_pg_height, 500) : # may need to increase if there are a ton of counties
        time.sleep(.05)
        d.execute_script(f"window.scrollTo(0, {y})") 
    
        links = d.find_elements_by_tag_name('a')
        links = [l.get_attribute('href') for l in links]
        all_links += links
    all_links = set(all_links)
    d.quit()
    
    # filter for URLs pointing to a county (they contain a double-slash!)
    county_urls = {h.split(f"//{state}/")[1].split('/')[0]: h
                   for h in links if f"//{state}/" in str(h)}
    county_urls = {k:v for k, v in county_urls.items() if k!=''}
    
    print(f'   > {len(county_urls)} URLs collected')
    config.county_urls = county_urls
    
    
# after pre-scrape, define directory names
def define_directories():
    
    # use today's date details for scraped_ child directory structure 
    config.mo_dy = 'nov_29' #dt.strftime(dt.today(), '%b_%d').lower()

    # use election date for parent directory structure
    e_date_dt = dt.strptime(config.race_date, '%B %d, %Y')
    config.yr_mo = dt.strftime(e_date_dt, '%Y_%B').lower()

    print('Target parent folder:', f"'{config.yr_mo}'")
    print('|— today:', f"'{config.mo_dy}'")

    # SET TARGET DIRECTORY FOR FINAL JOINED + CLEANED DATA HERE 
    config.target_cleaned_dir = f'../../../../data/{config.yr_mo}/candidate_votes_{config.mo_dy}'

    # define target directory (to store interim raw data + parsing)
    config.target_dir = os.path.join('../', config.yr_mo, 'scraped_'+config.mo_dy)


### DOWNLOAD THE TXT FILE FOR A GIVEN COUNTY
def scrape_county(county_name, target_dir):
    url = config.county_urls[county_name]
    config.d.get(url)
        
    # prompt
    for dl_try in range(3):
        try:
            time.sleep(2)
            config.d.execute_script(f"window.scrollTo(0, 1050)")
            dl_btn = config.d.find_element_by_css_selector("a[download='detailtxt.zip']")
            dl_btn.click()
            time.sleep(1)
            break
        except: pass
    
    # monitor
    download_dir = config.download_dir
    filename = max([f for f in os.listdir(download_dir)], # while downloading
                    key=lambda x : os.path.getctime(os.path.join(download_dir,x)))
    waits = 0
    while '.part' in filename or 'crdownload' in filename: # still downloading
        if waits > max_down_wait_sec*2:
            print("\n  [ERROR] - download timed out (max waits exceeded)!")
            raise TimeoutError()
        # re-define filename to check for 'part' or 'crdownload' (not done)
        filename = max([f for f in os.listdir(download_dir)],
                   key=lambda xa : os.path.getctime(os.path.join(download_dir,xa)))
        waits += 1
        time.sleep(.5)
        
    # rename & move downloaded file to target directory
    county_name += '.zip'
    os.rename(os.path.join(download_dir, filename),
              os.path.join(download_dir, county_name))
    time.sleep(.15)
    shutil.move(os.path.join(download_dir, county_name),
                os.path.join(target_dir, county_name))
    
    config.data_dir = target_dir
    

def scrape_all_counties():
    config.d = webdriver.Chrome()
    
    if not os.path.exists(config.target_dir):
        os.makedirs(config.target_dir)
        
    target_dir = os.path.join(config.target_dir, 'zips')
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)
    
    print('Scraping county results data...')
    max_attempts = 10
    for county in tqdm(list(config.county_urls.keys())):
        attempts = 0
        done = False
        o_len = len(os.listdir(target_dir))
        while True:
            if done or attempts > max_attempts: break
            attempts += 1
            try:
                scrape_county(county, target_dir)
                time.sleep(2)
                if len(os.listdir(target_dir)) != o_len:
                    done = True
            except:
                time.sleep(2)
           
    config.d.quit()
    print('—'*30, '\ndone!') 
    

# utility function
def ld_nh(path):
    for f in os.listdir(path):
        if not f.startswith('.'):
            yield f
def listdir_nohidden(path):
    return list(ld_nh(path))


def unzip_downloads():
    target_dir = os.path.join(config.target_dir, 'raw')
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)

    files = listdir_nohidden(config.data_dir)
    for file in tqdm(files):
        fpath = os.path.join(config.data_dir, file) # original filename
        zip_ref = zipfile.ZipFile(fpath, 'r')
        zip_ref.extractall(target_dir)
        zip_ref.close()
        time.sleep(.1)
        filename = max([f for f in listdir_nohidden(target_dir)],
                        key=lambda x : os.path.getctime(os.path.join(target_dir,x)))
        time.sleep(.1)
        os.rename(os.path.join(target_dir, filename),
                      os.path.join(target_dir, file.replace('.zip', '.txt')))
    
    print('>', len(listdir_nohidden(target_dir)), 'files unzipped')
    config.txt_dir = target_dir
    


    
### PARSE COUNTY RESULTS

def read_data(file):
    
    data_dir = config.txt_dir
    
    with open(os.path.join(data_dir, file)) as f:
        f = f.read()
    
    delim = lambda x: re.split('(?:\s){2,}', x) # delimit cells by 3 or more spaces
    rows = [delim(r) for r in f.split('\n')] # delimit rows with line breaks
    
    # function to get the category of each race
    # ( for organizing the repository )
    def parse_cat(race):
        if 'Service' in race:
            return ' '.join(race.split()[:3])
        else: return ' '.join(race.split()[:2]).replace('President of', 'US President')


    # find the consecutive batch of rows associated with each race
    data = [] # to compile info on all races

    for i, row in enumerate(rows[1:-2]):
        
        row_data = {}
        if row==['']: # indicates the beginning of data on a row

            if i>5: # identify the last row of & save the previous race
                last_row_data['data_ends'] = i-1
                last_row_data['data'] = rows[ last_row_data['data_starts'] : i ] 
                data.append(last_row_data)         

            if i < (len(rows)-10): # identify the first row of & other details on this race 
                row_data['race'] = ''.join(rows[i+2])
                row_data['race_cat'] = parse_cat(row_data['race'])
                row_data['candidates'] = rows[i+3]
                row_data['data_starts'] = i+4 
                last_row_data = row_data.copy()
    return data[1:]
    print(len(data), 'total races found.')
    
    
stats_should_be = ['Election Day Votes', 'Advanced Voting Votes', 'Absentee by Mail Votes',
                   'Provisional Votes', 'TOTAL VOTES']


def rename_cols(cols, candidates):
    # find sum total col
    for i, col in enumerate(cols):
        if col=='Total': final_total_col = i
            
    for i, col in enumerate(cols):
        if 'Total' in col and i!=final_total_col: cols[i] = 'Choice Total'

    new_cols = []
    
    for i, col in enumerate(cols):
        if i == final_total_col:
            new_cols.append(col)
        elif 'County' in col or 'Precinct' in col or 'Voters' in col or len(col)<3:
            new_cols.append(col)
        else:
            new_cols.append(candidates[0]+'_'+col)
            if 'Total' in col:
                candidates = candidates[1:]

    return new_cols


def parse_data(data, race_idx, county_name):
    race_data = pd.DataFrame(data[race_idx]['data'])
    race_data.columns = race_data.loc[0]
    race_data.drop(0, inplace=True)
    
    # most counties used "county" instead of "precinct" (all are actually precincts)
    if 'County' in race_data.columns:
        race_data.rename(columns={'County': 'Precinct'}, inplace=True) 
    race_data = race_data.set_index('Precinct')
    race_data['County'] = county_name.replace('_', ' ')


    
    # add candidate names to specific columns
    candidates = [c for c in data[race_idx]['candidates'] if len(c)>1]
    cols = list(race_data.columns)
    
    if False: # len(candidates)>5:
        print(county_name.upper())
        print(candidates)
        print(cols)
        print('\n\n\n')
    
    # run column renaming function, adding candidates to features
    new_cols = rename_cols(cols, candidates.copy())
    race_data.columns = new_cols.copy()
        
    # create directories & file details
    race_name = data[race_idx]['race'].split('/')[0]
    
    target_dir=os.path.join(config.target_dir, 'clean_by_county')
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)
    if county_name not in os.listdir(target_dir):
        os.makedirs(f'{target_dir}/{county_name}/')

    # save csv
    race_data = race_data[[c for c in race_data.columns if 'Unnamed' not in c]]
    race_data = race_data[['County'] + [c for c in race_data.columns if c!='County']]
    race_data.reset_index(inplace=True)
    race_name = race_name.replace('Special Election', 'Special').strip()
    race_name = race_name.replace('District', 'Dist').strip()

    race_data.to_csv(f'{target_dir}/{county_name}/{race_name}.csv', index=False)
        
def clean_county_data():
    files = listdir_nohidden(config.txt_dir)
    for file in tqdm(sorted(files)): # iterate counties
        data = read_data(file)

        for race_idx in range(len(data)): # iterate races within county
            parse_data(data.copy(), race_idx, file.split('.')[0])
            
            
            
# JOINING CODE


def list_all_races():
    all_races = []
    clean_dir = os.path.join(config.target_dir, 'clean_by_county')
    counties = listdir_nohidden(clean_dir)
    for county in counties:
        all_races += os.listdir( os.path.join(clean_dir, county) )
    all_races = set(all_races)
    print(len(all_races), 'total races identified')
    return all_races


            
    
# for a given race type:
def merge_race_files(race):
    print(f'Loading {race} data...')
    clean_dir = os.path.join(config.target_dir, 'clean_by_county')
    counties = listdir_nohidden(clean_dir)
    
    race_files = []
    for county in counties:
        county_dir = os.path.join(clean_dir, county)
        county_files = listdir_nohidden(county_dir)
        for file in county_files:
            if file==race: # filter for files in THIS (target) RACE
                race_files.append( os.path.join(county_dir, file) )
                
    found_pct = round(100*( len(race_files)/len(counties) ))
    print(' > Found data for', len(race_files), f'counties ({round(found_pct)}%)')

    if 90 < found_pct < 100.00:
        sus_missing = [c for c in counties if not
                       any(c in r for r in race_files)]
        print('   > Missing:', sus_missing)            

    # concatenate data for this race in all counties
    merged = pd.concat( [pd.read_csv(f) for f in race_files] )
    
    for c in merged.columns:
        if 'Unnamed' in c or c=='index':
            merged.drop(c, axis=1, inplace=True)
    merged.reset_index(drop=True, inplace=True)
    
    return merged


def prompt_typo_fixes(merged):
    # exclude: county, precinct, registered, voted
    data_cols = list(merged.columns)[3:-1] # candidate columns
    try: assert len(data_cols)%5==0 # should be 5 vote type categories per candidate
    except: print('Unexpected column layout:', data_cols)
    candidates = set([c.split('_')[0] for c in data_cols])
    remaining = candidates.copy()
    for cand in candidates: # iterate permutations of potential candidate typos
        others = remaining.copy()
        others.remove(cand)
        for compare in others:
            f_score = fuzz.ratio(cand, compare)
            spells = {'1': cand, '2': compare}
            
            if f_score > 85: # maybe match, ask for input
                print(f"\nPOSSIBLE typo found ({f_score}% match)")
                for i, c in spells.items():
                    print(f" > {i}. {c} — {int(merged[c+'_'+'Choice Total'].sum())} total votes")
                if 'y' not in input(
                    "  > Combine these candidates? (y/n):  ").lower():
                    continue
            else: continue
                    
            correct_idx = input("  > Which is correct? (1 or 2)  ").replace('.', '')
            correct_spell =  spells[correct_idx]
            typo_spell =  spells['12'.replace(correct_idx, '')]

            # MAKE THE CORRECTION
            correct_cols = [c for c in merged.columns if correct_spell in c]
            typo_cols = [c for c in merged.columns if typo_spell in c]
            total_added = 0 # for log
            for correct_col, typo_col in zip(correct_cols, typo_cols):
                merged[correct_col] = merged[correct_col].fillna(0)
                merged[typo_col] = merged[typo_col].fillna(0)
                total_added += merged[typo_col].sum() # log
                merged[correct_col] += merged[typo_col] # overwrite with addition
                merged.drop(typo_col, axis=1, inplace=True) # drop typo column

            print(f" > Added {int(total_added)} votes to {correct_spell}'s statewide total")
            print(f" > Removed {typo_spell}")
                    
        remaining.remove(cand) # for minimal iterations
        
        
# registered gets smushed into precinct name sometimes. fix with right-shift
def shift_fix(merged):
    for i in merged[merged.Total.isna()].index: #  no total = values shifted to left
        for feat, val in dict(merged.loc[i]).items(): 
            if feat=='Precinct':
                ns_found = re.findall('[\d]*', val)
                n_registered = [n for n in ns_found if n!=''][-1]
                parsed_prec = val.replace(n_registered, '').strip()

        # shift all values to the right
        merged.iloc[i:i+1, 2:] = merged.iloc[i:i+1, 2:].shift(1, axis=1)

        # parse registered & clean precinct
        merged.loc[i, 'Registered Voters'] = int(n_registered)
        merged.loc[i, 'Precinct'] = parsed_prec

    return merged


def check_math(merged):
    print('Verifying vote totals...')
    miscalcs = []
    
    all_cand_cols = [c for c in list(merged.columns)[3:] if c !='Total']
    candidates = set([c.split('_')[0] for c in all_cand_cols])
    for cand in tqdm(candidates):
        this_cand_cols = [c for c in all_cand_cols if cand in c]    
        this_cand_total = this_cand_cols[-1]
        this_cand_types = this_cand_cols[:-1]        
        for ridx in merged[merged[this_cand_total].notna()].index:
            act_sum = merged.loc[ridx, this_cand_total]            
            calc_sum = merged.loc[ridx][this_cand_types].sum()
            
            if calc_sum != act_sum: # check math
                miscalcs.append(ridx)
            
    pct_wrong = len(miscalcs) / (len(candidates)*len(merged))
    if miscalcs: print(f'>>> Vote totals verified — {round(pct_wrong, 5)} error rate')
    else: print(f'> Vote totals verified — no errors')
        
        
def get_part_rates(merged):
    merged=merged.copy()
    for ridx in merged.index:
        reg = merged.loc[ridx, 'Registered Voters']
        if reg!=0:
            try:
                voted = merged.loc[ridx, 'Total']
                part = round((int(voted)/int(reg))*100, 1)
                merged.loc[ridx, 'calc_participation'] = part
            except: pass
    return merged


def save(merged, race):
    target_dir = config.target_cleaned_dir

    # convert to numeric where possible
    for c in merged.columns:
        try:  merged[c] = merged[c].astype(float)
        except: pass
        
    # SAVE PRECINCT DATA
    sub_dir = 'by_precinct'
    if not os.path.exists(f'{target_dir}/{sub_dir}'):
        os.makedirs(f'{target_dir}/{sub_dir}')
    m = get_part_rates(merged)
    m.to_csv(
        os.path.join(target_dir, sub_dir, race), index=False)

    # SAVE COUNTY GROUPBY DATA
    sub_dir = 'by_county'
    if not os.path.exists(f'{target_dir}/{sub_dir}'):
        os.makedirs(f'{target_dir}/{sub_dir}')
    num_cols = merged._get_numeric_data().columns
    counties = merged.groupby('County').sum()
    counties = get_part_rates(counties)
    counties = counties.sort_values('County')
    
    counties.to_csv(  # index is county now, so don't drop.
        os.path.join(target_dir, sub_dir, race))
    
    
def join_all_race_data(target_races):
    
    races = [r for r in list_all_races() if
             any(t in r for t in target_races)]
    print(len(races), 'selected')
    
    for race in races:
        print('—'*60)
        merged = merge_race_files(race)
        prompt_typo_fixes(merged)
        merged = shift_fix(merged)
        check_math(merged)
        merged = get_part_rates(merged)
        save(merged, race)
        
        
    print('—'*60)
    print('COMPLETE')


