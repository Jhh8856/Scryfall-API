import os
import numpy as np
import pandas as pd
import json
import time

import warnings
warnings.filterwarnings("ignore")

features = ["card_faces",#card_faces should be features[0]
            "name",
            "printed_name",
            "mana_cost",
            "cmc",
            "type_line",
            "printed_type_line",
            "oracle_text",
            "printed_text",
            "power",
            "toughness",
            "loyalty",
            "colors"
            ]

features_en = ["card_faces",#card_faces should be features[0]
               "name", 
               "mana_cost", 
               "cmc", 
               "type_line", 
               "oracle_text", 
               "power", 
               "toughness",
               "loyalty",
               "colors"
               ]

features_customize = ['eng_name',
                      'chn_name',
                      'jpn_name',
                      'cost',
                      'mana',
                      'color',
                      'type',
                      'chn_type',
                      'pt',
                      'p',
                      't',
                      'loyalty',
                      'rule',
                      'chn_rule',
                      'back_name',
                      'created',
                      'update']

language = ['zht', 'en']

def download_bulk_data(mode=3):
    if mode == 0:#oracle unique
        file_name = "oracle_{}.json".format(time.strftime("%Y%m%d", time.localtime()))
    elif mode == 3:#all cards
        file_name = "all_cards_{}.json".format(time.strftime("%Y%m%d", time.localtime()))
    else:
        return ValueError()
    #more info:https://scryfall.com/docs/api/bulk-data
    
    if os.path.isfile("./{}".format(file_name)) == False:
        print("request scryfall bulk data...\n")    
    
        import requests
        bulk = requests.get("https://api.scryfall.com/bulk-data")
        
        if bulk.status_code == requests.codes.ok:
            print("done\n")            
            print("download:{}...\n".format(file_name))
            
            data = requests.get(json.loads(bulk.text)["data"][mode]["download_uri"], allow_redirects=True)
            with open(file_name, 'wb+') as f:
                f.write(data.content)       
            print("download complete\n")
            return file_name
        else:
            return requests.codes.ConnectionError
    else:
        print("data existed, skip download\n")
        return file_name

def json_to_df(name, lang):
    print("transfer...(language:{})\n".format(lang))
    
    cards_df = pd.DataFrame()
    with open(name.format(name), encoding="UTF-8") as data:
        lst = []
        cards = json.loads(data.read())
        for card in cards:
            if card["lang"] == lang:#take we need
                lst.append(card)
        #to json_normalize(), use addition space to process it faster
        cards_df = pd.json_normalize(lst)
        #for card in lst:
            #cards_df.append(pd.read_json(card))
    print("transfer complete\n")
    return cards_df

def saving(cards_df, rename):
    print("saving:{}.csv...\n".format(rename)) 
    
    cards_df.to_csv("{}.csv".format(rename), index=False, encoding="utf-8-sig")
    print("save complete\n")

def process(cards_df, features, lang):
    print("processing...\n")
    
    #take features we need
    cards_df = cards_df.loc[:, [feature for feature in features]]
    
    def preprocess(cards_df):
        #drop basic land
        cards_df = cards_df.loc[lambda df: df["name"] != "Plains"]
        cards_df = cards_df.loc[lambda df: df["name"] != "Island"]
        cards_df = cards_df.loc[lambda df: df["name"] != "Swamp"]
        cards_df = cards_df.loc[lambda df: df["name"] != "Mountain"]
        cards_df = cards_df.loc[lambda df: df["name"] != "Forest"]
        
        #double faces
        for i in range(cards_df.shape[0]):
            try:
                for feature in features:
                    try:
                        cards_df[feature][i] = cards_df["card_faces"][i][0][feature]
                    except KeyError:
                        continue
            except TypeError:
                continue
            
        cards_df = cards_df.reset_index(drop=True)#always do this after slice by iterator
        
        #cmc drop float
        cards_df["cmc"] = cards_df["cmc"]
        # cards_df["cmc"] = cards_df["cmc"].astype("int")
        
        #mana_cost
        cards_df["mana_cost"] = cards_df["mana_cost"].astype("str")
        cards_df.loc[:, ["mana_cost"]] = [ seq.replace("{", "").replace("}", "") for seq in cards_df["mana_cost"] ]
        
        #colors
        cards_df.loc[:, ["colors"]] = [ str(seq).strip("[]").replace(", ", "").replace("'", "") for seq in cards_df["colors"] ]
        cards_df.loc[:, ["colors"]] = [ "N" if x == "" else x for x in cards_df["colors"] ]
        cards_df.loc[:, ["colors"]] = [ "Z" if len(x) >= 2 else x for x in cards_df["colors"] ]
        cards_df.loc[:, ["colors"]] = [ "L" if cards_df["colors"][i] == "N" and "Land" in cards_df["type_line"][i] else cards_df["colors"][i] for i in range(cards_df.shape[0]) ]
        
        return cards_df
    
    #fill printed
    def fill(cards_df):
        if lang != "en":
            cards_df.loc[:, ["name", "printed_name"]] = cards_df.loc[:, ["name", "printed_name"]].ffill(axis=1)
            cards_df.loc[:, ["type_line", "printed_type_line"]] = cards_df.loc[:, ["type_line", "printed_type_line"]].ffill(axis=1)
            cards_df.loc[:, ["oracle_text", "printed_text"]] = cards_df.loc[:, ["oracle_text", "printed_text"]].ffill(axis=1)
        return cards_df
            
    def drop_card_face(cards_df):
        cards_df = cards_df.loc[:, [feature for feature in features[1:]]]#drop card_faces
        cards_df = cards_df.fillna("")
        return cards_df
    #unique
    def unique(cards_df):
        cards_df = cards_df.drop_duplicates(subset='name', keep='last')
        cards_df = cards_df.reset_index(drop=True)
        return cards_df
    
    #split strategy
    cards_df = preprocess(cards_df)
    if lang != 'en':
        df_full = cards_df.loc[lambda df: pd.notna(df["printed_name"]) == True]
        df_miss = cards_df.loc[lambda df: pd.isna(df["printed_name"]) == True]
        
        df_full = unique(drop_card_face(df_full))
        df_miss = unique(drop_card_face(fill(df_miss)))
        print("process complete\n")
        return df_full, df_miss
    elif lang == 'en':
        df = unique(drop_card_face(cards_df))
        print("process complete\n")
        return df
    else:
        print('language error')
        
def json2csv(file, lang=['zht']):
    #zht
    if 'zht' in lang:
        df_zht_full, df_zht_miss = process(json_to_df(file, "zht"), features, "zht")
        saving(df_zht_full, "zht")
        saving(df_zht_miss, "zht_missing")
    #en
    if 'en' in lang:
        df_en = process(json_to_df(file, "en"), features_en, "en")
        saving(df_en, "en")
    #ja
    if 'ja' in lang:
        df_jp_full, df_jp_miss = process(json_to_df(file, "ja"), features, "ja")
        saving(df_jp_full, "ja")
        saving(df_jp_miss, "ja_missing")
    #need more versions?

def customize(file_name, features=features_customize):
    df = pd.read_csv('{}.csv'.format(file_name))
    df_custom = pd.DataFrame(columns=features)
    
    df_custom['eng_name'] = df['name']
    df_custom['cost'] = df['mana_cost']
    df_custom['mana'] = df['cmc']
    df_custom['color'] = df['colors']
    df_custom['type'] = df['type_line']
    df_custom['p'] = df['power']
    df_custom['t'] = df["toughness"]
    df_custom['pt'] = df_custom['p']+'/'+df_custom['t']
    df_custom['pt'] = ['' if x == '/' else x for x in df_custom['pt']]
    df_custom['loyalty'] = df['loyalty']
    df_custom['rule'] = df['oracle_text']
    
    #zht printed
    if file_name == 'zht':
        df_custom['chn_name'] = df['printed_name']
        df_custom['chn_type'] = df['printed_type_line']
        df_custom['chn_rule'] = df['printed_text']
    #ja printed
    if file_name == 'ja':
        df_custom['jpn_name'] = df['printed_name']
        
    df_custom = df_custom.fillna("")
    saving(df_custom, 'custom_{}'.format(file_name))

jsonfile = download_bulk_data(mode=3)#https://scryfall.com/docs/api/bulk-data
#json2csv(jsonfile, lang=['zht', 'en', 'ja'])
json2csv(jsonfile, lang=language)
customize(file_name='en')
customize(file_name='zht')
customize(file_name='zht_missing')
#customize(file_name='ja')