import requests
from multiprocessing import Pool
from bs4 import BeautifulSoup as bs
import pandas as pd
from pandas import DataFrame
import csv
from pprint import pprint
import json
import os
import time
from selenium import webdriver
import time
import re
import ast

listeCles=['ville','lien']
dico = {
    **{i: '' for i in listeCles},
    **{ str(a) : '' for a in range(2003, 2020)},}



colonnes = list(dico.keys())

# fonction qui va nous permettre de faire la difference entre les elements scrapes et non scrapes
def diff(list1, list2):
    return list(set(list1).symmetric_difference(set(list2)))

# Parer a l'eventualite que le script s'est arreter
if os.path.isfile("C:/Users/lamine/PycharmProjects/dashboard1/dataset/chomage.csv"):
    tableauChomage = pd.read_csv('C:/Users/lamine/PycharmProjects/dashboard1/dataset/chomage.csv', error_bad_lines=False, dtype='unicode')
    tableauLiens = pd.read_csv('C:/Users/lamine/PycharmProjects/dashboard1/dataset/lienVille.csv')
    colonnes1 = tableauChomage['lien']
    colonnes2 = tableauLiens['lien']
    listeLiens = diff(colonnes1, colonnes2)
else:
    tableauChomage = DataFrame(columns = colonnes)
    tableauChomage.to_csv('C:/Users/lamine/PycharmProjects/dashboard1/dataset/chomage.csv', index=False)

    tableauLiens = pd.read_csv('C:/Users/lamine/PycharmProjects/dashboard1/dataset/lienVille.csv')
    listeLiens = tableauLiens['lien']

listeLiens = [lien for lien in listeLiens if str(lien)[:11] == '/management']

compteur = 0
for lien in listeLiens:
    with open('C:/Users/lamine/PycharmProjects/dashboard1/dataset/chomage.csv', 'a', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames= colonnes, lineterminator= '\n')
        ville = lien.split('/')[3]
        departement = lien.split('/')[-1][-5:-3]
        code_insee = lien.split('/')[-1][-5:]

        # Initialisation d'un dico
        dico = {
            **{i : '' for i in ['ville','lien']},
            **{str(annee) : '' for annee in range(2003,2020)}
        }
        dico['lien'] = lien
        dico['ville'] = tableauLiens[tableauLiens['lien'] == lien]['ville'].iloc[0]


        url="https://ville-data.com/chomage/" + '-'.join([ville,departement,code_insee])
        try:
            req=requests.get(url)
            contenu=req.content
            soup=bs(contenu,'html.parser')
            divs=soup.find('div',class_='main')
            script=divs.findAll('script')

            for s in script:
                if "data.addRows" in s.text:
                    html=s.text
                    l=html.splitlines()
        #scriptline = next((line for line in html.splitlines() if "data.addRows" in line))
        #print(scriptline)

                    liste=ast.literal_eval(l[8].strip(';').strip(')').strip(']').strip(','))

                    for i in liste:
                        cle=i[0]
                        valeur=float(i[1])
                        dico[cle] = valeur
            writer.writerow(dico)
        except:
            dico = {str(annee): '' for annee in range(2006, 2020)}
            dico['lien'] = lien
            dico['ville'] = tableauLiens[tableauLiens['lien'] == lien]['ville'].iloc[0]
            writer.writerow(dico)

        compteur = compteur + 1
        print(compteur)












