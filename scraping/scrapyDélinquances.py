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


colonnes = ['ville', 'lien', "Violences aux personnes", "Vols et d�gradations", "D�linquance �conomique et financi�re",
            "Autres crimes et d�lits","Violences gratuites", "Violences crapuleuses", "Violences sexuelles",
            "Menaces de violence", "Atteintes � la dignit�", "Cambriolages","Vols � main arm�e (arme � feu)",
            "Vols avec entr�e par ruse", "Vols li�s � l'automobile", "Vols de particuliers", "Vols d'entreprises",
            "Violation de domicile", "Destruction et d�gradations de biens", "Escroqueries, faux et contrefa�ons",
            "Trafic, revente et usage de drogues", "Infractions au code du Travail", "Infractions li�es � l'immigration",
            "Diff�rends familiaux", "Prox�n�tisme","Ports ou d�tentions d'arme prohib�e", "Recels",
            "D�lits des courses et jeux d'argent", "D�lits li�s aux d�bits de boisson et de tabac",
            "Atteintes � l'environnement", "D�lits li�s � la chasse et la p�che", "Cruaut� et d�lits envers les animaux",
            "Atteintes aux int�r�ts fondamentaux de la Nation"]

# fonction qui va nous permettre de faire la difference entre les elements scrapes et non scrapes
def diff(list1, list2):
    return list(set(list1).symmetric_difference(set(list2)))

# Parer a l'eventualite que le script s'est arreter
if os.path.isfile("C:/Users/lamine/PycharmProjects/dashboard1/dataset/delinquance.csv"):
    tableauDelinquance = pd.read_csv('C:/Users/lamine/PycharmProjects/dashboard1/dataset/delinquance.csv', error_bad_lines=False, dtype='unicode',encoding = "ISO-8859-1")
    tableauLiens = pd.read_csv('C:/Users/lamine/PycharmProjects/dashboard1/dataset/lienVille.csv')
    colonnes1 = tableauDelinquance['lien']
    colonnes2 = tableauLiens['lien']
    listeLiens = diff(colonnes1, colonnes2)
else:
    tableauDelinquance = DataFrame(columns = colonnes)
    tableauDelinquance.to_csv('C:/Users/lamine/PycharmProjects/dashboard1/dataset/delinquance.csv', index=False,encoding = "ISO-8859-1")

    tableauLiens = pd.read_csv('C:/Users/lamine/PycharmProjects/dashboard1/dataset/lienVille.csv')
    listeLiens = tableauLiens['lien']

listeLiens = [lien for lien in listeLiens if str(lien)[:11] == '/management']


def parse(lien):
    dico = {i : '' for i in colonnes}
    dico['lien'] = lien
    dico['ville'] = tableauLiens[tableauLiens['lien'] == lien]['ville'].iloc[0]

    req = requests.get("http://www.linternaute.com/actualite/delinquance/" + lien[18:])
    time.sleep(2)
    if req.status_code == 200:
        with open('C:/Users/lamine/PycharmProjects/dashboard1/dataset/delinquance.csv', 'a', encoding = "utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames= colonnes, lineterminator='\n')
            contenu = req.content
            soup = bs(contenu, "html.parser")

            tables = soup.findAll('table', class_="odTable odTableAuto")

            if len(tables) > 0:
                for i in range(len(tables)):
                    for info in tables[i].findAll('tr')[1:]:
                        cle = info.findAll('td')[0].text
                        valeur = info.findAll('td')[1].text
                        try:
                            dico[cle] = float(''.join(valeur.split()).split('c')[0])
                        except:
                            dico[cle] = 'nc'

            writer.writerow(dico)
            print("[delinquance]", lien)

if __name__ == "__main__":
    with Pool(10) as p:
        p.map(parse, listeLiens)
