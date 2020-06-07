
import requests
from bs4 import BeautifulSoup as bs
import pandas as pd
import csv
from pandas import DataFrame
import os
from multiprocessing import Pool
import time



colonnes = ['ville', 'lien', 'Région', 'Département',
                'Etablissement public de coopération intercommunale (EPCI)',
                'Code postal (CP)','Code Insee',
                'Nom des habitants',
                'Population (2017)',
                'Population : rang national (2017)',
                'Densité de population (2017)',
                'Taux de chômage (2016)',
                'Pavillon bleu',
                "Ville d'art et d'histoire",
                'Ville fleurie',
                'Ville internet',
                'Superficie (surface)',
                'Altitude min.',
                'Altitude max.',
                'Latitude',
                'Longitude' ]
# fonction qui va faire la diffrence entre la liste des liens
def diff(list1, list2):
    return list(set(list1).symmetric_difference(set(list2)))


#le fichier existe ou pas
if os.path.isfile('C:/Users/lamine/PycharmProjects/dashboard1/dataset/info.csv'):
    #instruction


    tableauInfos=pd.read_csv('C:/Users/lamine/PycharmProjects/dashboard1/dataset/info.csv',error_bad_lines=False, dtype='unicode')#,error_bad_lines=False,encoding = "ISO-8859-1 encoding='CP1252',error_bad_lines=False"
    #error_bad_line = false c'est de passé si il trouve une erreur sur ligne
    colonnes1 = tableauInfos['lien']
    tableauLiens=pd.read_csv('C:/Users/lamine/PycharmProjects/dashboard1/dataset/lienVille.csv')
    colonnes2 = tableauLiens['lien']
    listeLiens = diff(colonnes1, colonnes2)
else:
    #instuction

    # creation de notre cvs info
    tableauInfos = DataFrame(columns=colonnes)
    tableauInfos.to_csv('C:/Users/lamine/PycharmProjects/dashboard1/dataset/info.csv', index=False)
    # recupere la liste des liens a scapy
    tableauLiens=pd.read_csv('C:/Users/lamine/PycharmProjects/dashboard1/dataset/lienVille.csv')
    listeLiens = tableauLiens['lien']

listLiens=[i for i in listeLiens if str(i[:11])=='/management']

#print(len(listLiens))

def parse(lien):
    #initialisation d'un dictionnaire
    dico = {i: '' for i in colonnes}
    colonne=dico.keys()
    # url du site (site + lien pour la ville)
    req = requests.get("http://www.journaldunet.com" + lien)
    # requete pour se concecter au url

    # pose de 2 seconde apres l'exucution des ligne d'avant
    time.sleep(4)
    if req.status_code==200: # satuts code =200 c'est il s'est conecter à la page ou pas (404 non exist .....)
        with open ('C:/Users/lamine/PycharmProjects/dashboard1/dataset/info.csv','a',encoding = "utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=colonne,lineterminator ='\n')

            contenu=req.content
            soup=bs(contenu,'html.parser')
            tables = soup.findAll('table', class_='odTable odTableAuto')

            dico['lien'] = lien
            dico['ville'] = tableauLiens[tableauLiens['lien'] == lien]['ville'].iloc[0]

            for i in range(len(tables)):
                tousLesTr = tables[i].findAll('tr')
                for tr in tousLesTr[1:]:
                    cle = tr.findAll('td')[0].text
                    valeur = tr.findAll('td')[1].text

                    if "Nom des habitants" in cle:
                        dico["Nom des habitants"] = str(valeur)
                    elif "Taux de chômage" in cle:
                        dico["Taux de chômage (2016)"] = str(valeur)
                    else:
                        dico[cle] = str(valeur)
            time.sleep(1)
            writer.writerow(dico)
            print(dico)

if __name__ == "__main__":
    with Pool(15) as p:
        p.map(parse, listeLiens)






