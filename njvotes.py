''' Useful code for cleaning up New Jersey voting datasets

Author: Sara Terp
Date: February 2017
'''

import pandas as pd
import csv


''' Get voting data headers
'''
def get_voteheaders():
    fin = open('../2017 Voting data/data headers.csv', 'r')
    cin = csv.reader(fin)
    voteheaders = cin.__next__()
    fin.close()
    return voteheaders


''' Get voting data headers, and set all of them to be string types (solves a lot of mess later on)
'''
def get_voteheadertypes():
    voteheaders = get_voteheaders()
    voteheadertypes = {}
    for col in voteheaders:
        voteheadertypes[col] = 'str'
    return voteheadertypes

''' Get voter header types
'''
def get_voterheadertypes():
    voterheaders = ['voter id', 'status code', 'party code', 'last name', 'first name',
       'middle name', 'prefix', 'suffix', 'sex', 'street number', 'suffix a',
       'suffix b', 'street name', 'apt/unit no.', 'address line 1',
       'address line 2', 'city', 'state', 'zip5', 'zip4',
       'mailing street number', 'mailing suffix a', 'mailing suffix b',
       'mailing street name', 'mailing apt/unit no.', 'mailing address line 1',
       'mailing address line 2', 'mailing city', 'mailing state',
       'mailing country', 'mailing zip code', 'birth date', 'date registered',
       'county precinct', 'municipality', 'ward', 'district', 'phone number',
       'election date', 'election name', 'election type', 'election category',
       'ballot type', 'county']
    voterheadertypes = {}
    for col in voterheaders:
        voterheadertypes[col] = 'str'
    return voterheadertypes
    

''' Format dataframe for NationBuilder upload
Nationbuilder format notes are in http://nationbuilder.com/fields_available_for_import
'''
def format_for_nationbuilder(df):

    df.loc[df['sex']=='N', ['sex']] = 'O'
    
    df.loc[df['party code']=='CNV', ['party code']] = 'O'
    df.loc[df['party code']=='CON', ['party code']] = 'C'
    df.loc[df['party code']=='DEM', ['party code']] = 'D'
    df.loc[df['party code']=='GRE', ['party code']] = 'G'
    df.loc[df['party code']=='REP', ['party code']] = 'R'
    df.loc[df['party code']=='UNA', ['party code']] = 'U'
    df.loc[df['party code']=='LIB', ['party code']] = 'L'
    df.loc[df['party code']=='NAT', ['party code']] = 'O'
    df.loc[df['party code']=='RFP', ['party code']] = 'E'
    df.loc[df['party code']=='SSP', ['party code']] = 'O'
    
    # Make sure we have the right address lines 1,2 and 3
    df.rename(columns={'address line 2': 'Address Line 3'}, inplace=True)
    df.rename(columns={'address line 1': 'Address Line 2'}, inplace=True)
    df['Address Line 1'] = df['street number'] + ' ' + df['street name']
    df['Address Line 1'].str.strip()
    df['Address Line 2'] = df['apt/unit no.'] + ' ' + df['Address Line 2']
    df['Address Line 2'].str.strip()

    return df


''' Format dataframe for NGP VAN upload

DGP VAN team makes you guess what the expected formats should be "oh no, we don't have them written down anywhere"
'''
def format_for_ngpvan(df):
    
    df['Address Type'] = 'Voting'
    
    for col in ['last name', 'first name', 'middle name', 'prefix', 'suffix',
                'street name', 'city', 'election name', 'county', 'municipality']:
        df[col] = df[col].str.title()

    allowedprefix = ['Mr.', 'Mrs.', 'Miss', 'Ms.', '']
    df.loc[df['prefix'].isin(allowedprefix) == False, ['prefix']] = ''
    
    df.loc[df['sex']=='M', ['sex']] = 'Male'
    df.loc[df['sex']=='F', ['sex']] = 'Female'
    df.loc[df['sex']=='N', ['sex']] = 'Other'
    
    df.loc[df['party code']=='CNV', ['party code']] = 'Conservative'
    df.loc[df['party code']=='CON', ['party code']] = 'Constitution'
    df.loc[df['party code']=='DEM', ['party code']] = 'Democrat'
    df.loc[df['party code']=='GRE', ['party code']] = 'Green'
    df.loc[df['party code']=='REP', ['party code']] = 'Republican'
    df.loc[df['party code']=='UNA', ['party code']] = 'Non-Partisan'
    df.loc[df['party code']=='LIB', ['party code']] = 'Liberal'
    df.loc[df['party code']=='NAT', ['party code']] = 'Other'
    df.loc[df['party code']=='RFP', ['party code']] = 'Other'
    df.loc[df['party code']=='SSP', ['party code']] = 'Other'

    df.loc[df['phone number'].str.len() < 10, ['phone number']] = ''
    
    df.rename(columns={'address line 2': 'Address Line 3'}, inplace=True)
    df.rename(columns={'address line 1': 'Address Line 2'}, inplace=True)
    df['Address Line 1'] = df['street number'] + ' ' + df['street name']
    df['Address Line 2'] = df['apt/unit no.'] + ' ' + df['Address Line 2']
    df['Mailing address'] = df['mailing street number'] + ' ' + \
    df['mailing street name'] + ',' + df['mailing apt/unit no.'] + ' ' + \
    df['mailing address line 1'] + ' ' + df['mailing address line 2'] + \
    df['mailing city'] + ' ' + df['mailing state'] + ' ' + \
    df['mailing country'] + ' ' + df['mailing zip code']
    df.loc[df['Mailing address'] == ' ,     ', ['Mailing address']] = ''
    
    df.rename(columns={'DEM': 'Voted as DEM', 'voter id': 'Voter Id',
                      'birth date' : 'Birthdate', 'sex': 'Sex',
                      'party code': 'Party', 'state': 'State',
                      'zip5': 'Zip'}, inplace=True)

    for col in ['street name', 'street number', 'suffix a', 'suffix b',
               'apt/unit no.', 'zip4', 'mailing street number', 
               'mailing street name', 'mailing apt/unit no.',
               'mailing address line 1', 'mailing address line 2',
               'mailing city', 'mailing state', 'mailing country', 
               'mailing zip code']:
        df = df.drop(col, axis=1)

    return df


''' Write voters files out in 'chunks' so they can be uploaded to NGP VAN
NGP only takes 10Mbyte datafiles. 
So split each county into 50k-people sized chunks
Even that is a bit too big according to their team: they suggested something 
around 10-15k people sized chunks (not sure of number, was too busy gasping at
the time). 
'''
def write_ngpvan(df, fileprefix):
    
    blocksize = 15000
    numblocks = math.ceil(len(df)/blocksize)
    for i in range(0, numblocks):
        
        outfile = fileprefix + str(i) + '.csv'
        df[i*blocksize:(i+1)*blocksize].to_csv(outfile, index=False)

        outfilelite = fileprefix + '_lite' + str(i) + '.csv'
        df[['Voter Id', 'Party', 'last name', 'first name', 'middle name',
            'prefix', 'suffix', 'Sex', 'Address Line 2', 'Address Line 3', 
            'city', 'Zip', 'Birthdate', 'phone number', 'county',
            'Address Type', 'Address Line 1']][i*blocksize:(i+1)*blocksize].to_csv(outfilelite, index=False)
        
    return


''' Add the number of times someone voted dem and non-dem to their record
'''
def add_votecounts(df):
    
    votecounts = pd.crosstab(df['voter id'], df['party code'])
    votecounts['Voted as nonDEM'] = votecounts.sum(axis=1) - votecounts['DEM']
    votecounts['voter id'] = votecounts.index
    df = pd.merge(df, votecounts[['voter id', 'DEM', 'Voted as nonDEM']])

    return df

def set_citycorrections():
    
    citycorrections = {
    'ATLANTIC': {
        '107103018': 'atlantic city',
        " twp": " township",
        " two": " township",
        "gallowat": "galloway",
        "w atlantic city": "west atlantic city",
        "eg harbor": "egg harbor",
        "eht": "egg harbor township",
        "buean": "buena",
        "mayslanding": "mays landing"
    },
    'BERGEN': {
        " park.": " park",
        "twp ": "township ",
        "woodridge": "wood ridge"
    },
    'BURLINGTON': {
        "eastampt0n": "eastampton",
        "jobstownn": "jobstown",
        "  ": " ",
        "mt ": "mount ",
        "mt. ": "mount ",
        "southamptonn": "southampton"
    },
    'CAMDEN': {
        '08009': 'berlin',
        '0aklyn': 'oaklyn',
        'acto': 'atco',
        'belrin': 'berlin',
        ' twp': ' township',
        'cherrt ': 'cherry ',
        'mt ': 'mount ',
        ' ephrain': ' ephraim',
        'sicklervillw': 'sicklerville',
        'w berlin': 'west berlin',
        'w collingswood': 'west collingswood',
        'waterfrod': 'waterford'
    },
    'CAPE MAY': {
        '08230': 'ocean view',
        '08243': 'sea isle city'
    },
    'CUMBERLAND': {
        'bridgton': 'bridgeton'
    },
    'ESSEX': {}, # Genuinely has no errors
    'GLOUCESTER': {
        '  ': ' ',
        'ave': 'wenonah',
        'deprford': 'deptford',
        'deptfprd': 'deptford',
        'framklinville': 'franklinville',
        'franklinvill': 'franklinville',
        ' twp': ' township',
        ' twp.': ' township',
        ' twsp': ' township',
        'mantuas': 'mantua',
        'manuta': 'mantua',
        'mqntua': 'mantua',
        'mickelton': 'mickleton',
        'mt ': 'mount ',
        ' roayl': ' royal',
        'mulica': 'mullica',
        'mullcia': 'mullica',
        'mulliac': 'mullica',
        'mullic ': 'mullica ',
        ' hil$': ' hill',
        'mullilca': 'mullica',
        'mulllica': 'mullica',
        'natioanl': 'national',
        'newfiled': 'newfield',
        'rd  thorofare': 'west deptford',
        'sickleville': 'sicklerville',
        'swewesboro': 'swedesboro',
        'turnersvll': 'turnersville',
        'turnesrville': 'turnersville',
        'turnesville': 'turnersville',
        'turnresville': 'turnersville',
        'wdbury hts': 'woodbury heights',
        'woodbury hgts': 'woodbury heights',
        'woodbury htgs': 'woodbury heights',
        'woodbury hts': 'woodbury heights',
        'woodbury hts.': 'woodbury heights',
        'wodbury': 'woodbury',
        'wenoanh': 'wenonah',
        'westvlle': 'westville',
        'wililamstown': 'williamstown',
        'willaimstown': 'williamstown',
        'williamston': 'williamstown',
        'williamstow': 'williamstown',
        'wooldwich': 'woolwich',
        'wooldwich': 'woolwich',
        'woolwivh': 'woolwich'
    },
    'HUDSON': {}, # Genuinely has no errors
    'HUNTERDON': {
        ' hts': ' heights'
    },
    'MERCER': {
        'hamilon': 'hamilton',
        'hamliton': 'hamilton',
        'pinceton': 'princeton',
        ' jct': ' junction'
    },
    'MIDDLESEX': {
        ' hts': ' heights',
        ' jct': ' junction',
        ' twp': ' township'
    },
    'MONMOUTH': {
        '  ': ' ',
        ' twp': ' township'
    },
    'MORRIS': {
        "  ": " ",
        "mt ":"mount ", 
        "mt.": "mount", 
        "mountarlington": "mount arlington",
        "par-troy hills": "parsippany-troy hills",
        " twp": " township", 
        " hts": " heights",
        "new foundland": "newfoundland",
        "pomptain": "pompton"},
    'OCEAN': {
        ' lt': ' light',
        ' bch': ' beach',
        'pt ': 'port ',
        ' hts': ' heights',
        ' pk': ' park'
    },
    'PASSAIC': {
        '  ': ' ',
        '07470': 'wayne',
        'new foundland': 'newfoundland'
    },
    'SALEM': {
        ' twp': ' township'
    },
    'SOMERSET': {
        '   ': ' ',
        '  ': ' ',
        ' twp': ' township',
        ' hts': ' heights',
        '^e ': 'east',
        'princteon': 'princeton',
        ' sta': ' station',
        'zarapath': 'zarephath'
    },
    'SUSSEX': {
        '07461': 'vernon'
    },
    'UNION': {
        ' hgts': 'heights',
        ' hts': ' heights',
        'berkely': 'berkeley',
        'elizabrth': 'elizabeth',
        'kemilworth': 'kenilworth',
        'llinden': 'linden',
        'palinfield': 'plainfield',
        'rosele': 'roselle',
        'scotchplains': 'scotch plains',
        'vuaxhall': 'vauxhall'
    },
    'WARREN': {
        'stewarsville': 'stewartsville',
        'stewartville': 'stewartsville'
    }}

    return citycorrections
