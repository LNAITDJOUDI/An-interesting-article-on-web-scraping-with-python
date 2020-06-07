#!/usr/bin/env python
# -*- coding: latin-1 -*-
import requests
from bs4 import BeautifulSoup as bs
import pandas as pd
import csv
from pandas import DataFrame

# colonnes de notre data set
colonnes=['ville','lien']
# creation d'un data frame
tableau=DataFrame(columns=colonnes)
# sauvgarder le csv
tableau.to_csv('C:/Users/lamine/PycharmProjects/dashboard1/dataset/lienVille.csv',index=False)
dico={}
dico['ville']=''
dico['lien']=''
url="http://www.journaldunet.com/management/ville/index/villes?page="


with open('C:/Users/lamine/PycharmProjects/dashboard1/dataset/lienVille.csv','a',encoding = "ISO-8859-1") as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=colonnes, lineterminator='\n')
    for numeroPage in range(1,701):
        print('le numero de page en traitement est : '+ str(numeroPage))
        req = requests.get(url+str(numeroPage))
        contenu = req.content
        soup = bs(contenu, 'html.parser')
        tousLeslines = soup.findAll('a')
        for lien in tousLeslines:
            if '/ville-'in lien['href'] and lien['href'][:11]=='/management':
                dico['lien']=lien['href']
                dico['ville']=lien.text

                writer.writerow(dico)

