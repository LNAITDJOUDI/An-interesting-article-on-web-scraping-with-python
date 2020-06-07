#!/usr/bin/env python
# -*- coding: latin-1 -*-
import requests
from bs4 import BeautifulSoup as bs
import pandas as pd
import time
from multiprocessing import Pool
import csv
from pandas import DataFrame
import os
import re


listCles=['ville','lien','Nathalie LOISEAU', 'Yannick JADOT', 'François-Xavier BELLAMY', 'Raphaël GLUCKSMANN', 'Jordan BARDELLA',
          'Manon AUBRY', 'Benoît HAMON', 'Ian BROSSAT', 'Jean-Christophe LAGARDE', 'Dominique BOURG', 'Hélène THOUY',
          'Nicolas DUPONT-AIGNAN', 'François ASSELINEAU', 'Florie MARIE', 'Nathalie ARTHAUD', 'Florian PHILIPPOT', 'Francis LALANNE',
          'Nagib AZERGUI', 'Sophie CAILLAUD', 'Nathalie TOMASINI', 'Olivier BIDOU', 'Yves GERNIGON', 'Pierre DIEUMEGARD', 'Christian Luc PERSON',
          'Thérèse DELFEL', 'Audric ALEXANDRE', 'Hamada TRAORÉ', 'Robert DE PREVOISIN', 'Vincent VAUCLIN', 'Gilles HELGEN',
          'Antonio SANCHEZ', 'Renaud CAMUS', 'Christophe CHALENÇON', 'Cathy Denise Ginette CORBET','Taux de participation',"Taux d'abstention",
          "Votes blancs (en pourcentage des votes exprimés)","Votes nuls (en pourcentage des votes exprimés)","Nombre de votants"]
dico={i:'' for i in listCles}
colonnes = list(dico.keys())
url="https://election-europeenne.linternaute.com/resultats/"
def diff(list1, list2):
    return list(set(list1).symmetric_difference(set(list2)))


if os.path.isfile('C:/Users/lamine/PycharmProjects/dashboard1/dataset/elections.csv'):
    tableauSante = pd.read_csv('C:/Users/lamine/PycharmProjects/dashboard1/dataset/elections.csv', error_bad_lines=False, dtype='unicode',encoding = "ISO-8859-1")
    colonnes1 = tableauSante['lien']
    tableauLiens = pd.read_csv('C:/Users/lamine/PycharmProjects/dashboard1/dataset/lienVille.csv',encoding = "ISO-8859-1")
    colonnes2 = tableauLiens['lien']
    listeLiens = diff(colonnes1, colonnes2)
else:
    # Creation de notre csv infos
    tableauSante = DataFrame(columns=colonnes)
    tableauSante.to_csv('C:/Users/lamine/PycharmProjects/dashboard1/dataset/elections.csv', index=False,encoding = "ISO-8859-1")
    # Je recupere la liste des liens a scraper
    tableauLiens = pd.read_csv('C:/Users/lamine/PycharmProjects/dashboard1/dataset/lienVille.csv',encoding = "ISO-8859-1")
    listeLiens = tableauLiens['lien']
url="https://election-europeenne.linternaute.com/resultats/"
listeLiens = [lien for lien in listeLiens if str(lien)[:11] == '/management']
def parse(lien):
    dico = {i: '' for i in listCles}
    dico['lien'] =lien
    dico['ville'] = tableauLiens[tableauLiens['lien'] == lien]['ville'].iloc[0]
    req=requests.get(url+lien[18:])
    time.sleep(4)
    if req.status_code == 200:
        with open('C:/Users/lamine/PycharmProjects/dashboard1/dataset/elections.csv','a',encoding = "ISO-8859-1") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=colonnes, lineterminator='\n')
            contenu=req.content
            soup=bs(contenu,'html.parser')
            divs=soup.findAll('div', class_="marB20")
            tableau=divs[3]
            candidats= tableau.findAll('tr',class_=re.compile('color'))

            for candidat in candidats :
                cle=candidat.find('strong').text
                valeur=candidat.findAll('td')[1].text.replace(',','.').replace('%','')
                try:
                    dico[cle]=float(valeur)
                except:
                    dico[cle] = valeur

            tables=tableau.findAll('table')

            if len(tables)==2:
                for info in tables[1].findAll('tr')[1:]:
                    cle=info.findAll('td')[0].text
                    valeur=info.findAll('td')[1].text.replace(',','.').replace('%','').replace(' ','')
                    try:
                        dico[cle] = float(valeur)
                    except:
                        dico[cle]=valeur
            writer.writerow(dico)
            time.sleep(2)
            print("[election]", lien)

if __name__ == "__main__":
    with Pool(10) as p:
        p.map(parse, listeLiens)


