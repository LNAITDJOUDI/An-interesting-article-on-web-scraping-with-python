#!/usr/bin/env python
# -*- coding: latin-1 -*-
from bs4 import BeautifulSoup as bs
from multiprocessing import Pool
import pandas as pd
from pandas import DataFrame
from pprint import pprint
import json
import requests
import time
import csv
import os
import re

colonnes = ['ville', 'lien', 'prix_m2', 'Nombre de logements', "Nombre moyen d'habitant(s) par logement",
            'R�sidences principales',
            'R�sidences secondaires', 'Logements vacants', 'Maisons', 'Appartements', 'Autres types de logements',
            'Propri�taires',
            'Locataires', '- dont locataires HLM', "Locataires h�berg�s � titre gratuit", 'Studios', '2 pi�ces',
            '3 pi�ces', '4 pi�ces', '5 pi�ces et plus','R�sidences principales de moins de 30 m�',
            'R�sidences principales de 30 m� � 40 m�','R�sidences principales de 40 m� � 60 m�',
            'R�sidences principales de 60 m� � 80 m�','R�sidences principales de 80 m� � 100 m�',
            'R�sidences principales de 100 m� � 120 m�','R�sidences principales de 120 m� et plus',
            'Temps pass� en moyenne dans un logement','Temps pass� par des propri�taires dans leur logement',
            "Temps pass� par des locataires HLM dans leur logement","Temps pass� dans un logement � titre gratuit",
            "Nombre de r�sidences principales construites avant 1919","Nombre de r�sidences principales construites avant 1919"
            ,"Nombre de r�sidences principales construites de 1919 � 1945","Nombre de r�sidences principales construites de 1946 � 1970",
            "Nombre de r�sidences principales construites de 1971 � 1990","Nombre de r�sidences principales construites de 1991 � 2005",
            "Nombre de r�sidences principales construites apr�s 2005",'Temps pass� par des locataires dans leur logement']


# fonction qui va nous permettre de faire la difference entre les elements scrapes et non scrapes
def diff(list1, list2):
    return list(set(list1).symmetric_difference(set(list2)))


# Parer a l'eventualite que le script s'est arreter
if os.path.isfile("C:/Users/lamine/PycharmProjects/dashboard1/dataset/immobilier.csv"):
    tableauImmo = pd.read_csv('C:/Users/lamine/PycharmProjects/dashboard1/dataset/immobilier.csv', error_bad_lines=False, dtype='unicode',encoding = "ISO-8859-1")
    tableauLiens = pd.read_csv('C:/Users/lamine/PycharmProjects/dashboard1/dataset/lienVille.csv')
    colonnes1 = tableauImmo['lien']
    colonnes2 = tableauLiens['lien']
    listeLiens = diff(colonnes1, colonnes2)
else:
    tableauImmo = DataFrame(columns=colonnes)
    tableauImmo.to_csv('C:/Users/lamine/PycharmProjects/dashboard1/dataset/immobilier.csv', index=False,encoding = "ISO-8859-1")
    tableauLiens = pd.read_csv('C:/Users/lamine/PycharmProjects/dashboard1/dataset/lienVille.csv')
    listeLiens = tableauLiens['lien']

listeLiens = [lien for lien in listeLiens if str(lien)[:11] == '/management']


def parse(lien):
    dico = {i: '' for i in colonnes}
    dico['lien'] = lien
    dico['ville'] = tableauLiens[tableauLiens['lien'] == lien]['ville'].iloc[0]

    req = requests.get("http://www.journaldunet.com" + lien + "/immobilier")
    time.sleep(2)
    if req.status_code == 200:
        with open('C:/Users/lamine/PycharmProjects/dashboard1/dataset/immobilier.csv', 'a', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=colonnes, lineterminator='\n')
            contenu = req.content
            soup = bs(contenu, "html.parser")

            try:
                js_script = soup.findAll('script')[6].string
                json_prix = json.loads(js_script)
                try:
                    dico['prix_m2'] = float(json_prix['series'][0]['data'][0])
                except:
                    dico['prix_m2'] = json_prix['series'][0]['data'][0]
            except:
                dico['prix_m2'] = 'nc'

            tables = soup.findAll('table', class_="odTable odTableAuto")

            if len(tables) > 0:
                for i in range(len(tables)):
                    for info in tables[i].findAll('tr')[1:]:
                        cle = info.findAll('td')[0].text
                        valeur = info.findAll('td')[1].text
                        if "Locataires h�berg�s" in cle:
                            try:
                                dico["Locataires h�berg�s � titre gratuit"] = float(
                                    ''.join(valeur.split()).replace(',', '.'))
                            except:
                                dico["Locataires h�berg�s � titre gratuit"] = valeur
                        elif "5 pi�ces" in cle:
                            try:
                                dico["5 pi�ces et plus"] = float(''.join(valeur.split()).replace(',', '.'))
                            except:
                                dico["5 pi�ces et plus"] = valeur
                        elif "Temps pass�" in cle:
                            try:
                                dico[cle] = float((valeur.split())[0].replace(',','.'))
                            except:
                                dico[cle] = valeur
                        else:
                            try:
                                dico[cle] = float(''.join(valeur.split()).replace(',', '.'))
                            except:
                                dico[cle] = valeur

            writer.writerow(dico)
            print("[immo]", lien)


if __name__ == "__main__":
    with Pool(10) as p:
        p.map(parse, listeLiens)
