#!/usr/bin/env python
# -*- coding: latin-1 -*-
import requests
from multiprocessing import Pool
from bs4 import BeautifulSoup as bs
import pandas as pd
from pandas import DataFrame
import csv
import json
import os
import time
from pprint import pprint





listeCles = ["ville", "lien", "Allocataires CAF", "B�n�ficiaires du RSA", " - b�n�ficiaires du RSA major�",
             " - b�n�ficiaires du RSA socle", "B�n�ficiaires de l'aide au logement",
             " - b�n�ficiaires de l'APL (aide personnalis�e au logement)",
             " - b�n�ficiaires de l'ALF (allocation de logement � caract�re familial)",
             " - b�n�ficiaires de l'ALS (allocation de logement � caract�re social)",
             " - b�n�ficiaires de l'Allocation pour une location immobili�re",
             " - b�n�ficiaires de l'Allocation pour un achat immobilier", "B�n�ficiaires des allocations familiales",
             " - b�n�ficiaires du compl�ment familial",
             " - b�n�ficiaires de l'allocation de soutien familial",
             " - b�n�ficiaires de l'allocation de rentr�e scolaire", "B�n�ficiaires de la PAJE",
             " - b�n�ficiaires de l'allocation de base",
             " - b�n�ficiaires du compl�ment mode de garde pour une assistante maternelle",
             " - b�n�ficiaires du compl�ment de libre choix d'activit� (CLCA ou COLCA)",
             " - b�n�ficiaires de la prime naissance ou adoption", "M�decins g�n�ralistes",
             "Masseurs-kin�sith�rapeutes", "Dentistes", "Infirmiers",
             "Sp�cialistes ORL", "Ophtalmologistes", "Dermatologues", "Sage-femmes", "P�diatres", "Gyn�cologues",
             "Pharmacies", "Urgences", "Ambulances",
             "Etablissements de sant� de court s�jour", "Etablissements de sant� de moyen s�jour",
             "Etablissements de sant� de long s�jour", "Etablissement d'accueil du jeune enfant",
             "Maisons de retraite", "Etablissements pour enfants handicap�s", "Etablissements pour adultes handicap�s"]

dico = {
    **{i: '' for i in listeCles},
    **{"nbre allocataires (" + str(a) + ")": '' for a in range(2009, 2018)},
    **{"nbre RSA (" + str(a) + ")": '' for a in range(2009, 2018)},
    **{"nbre APL (" + str(a) + ")": '' for a in range(2009, 2018)},
    **{"nbre Alloc Familiales (" + str(a) + ")": '' for a in range(2009, 2018)},
}

colonnes = list(dico.keys())


# fonction qui va faire la difference entre les liens scrapes et non scrapes
def diff(list1, list2):
    return list(set(list1).symmetric_difference(set(list2)))


if os.path.isfile('C:/Users/lamine/PycharmProjects/dashboard1/dataset/santeSocial.csv'):
    tableauSante = pd.read_csv('C:/Users/lamine/PycharmProjects/dashboard1/dataset/santeSocial.csv', error_bad_lines=False, dtype='unicode')
    colonnes1 = tableauSante['lien']
    tableauLiens = pd.read_csv('C:/Users/lamine/PycharmProjects/dashboard1/dataset/lienVille.csv')
    colonnes2 = tableauLiens['lien']
    listeLiens = diff(colonnes1, colonnes2)
else:
    # Creation de notre csv infos
    tableauSante = DataFrame(columns=colonnes)
    tableauSante.to_csv('C:/Users/lamine/PycharmProjects/dashboard1/dataset/santeSocial.csv', index=False)
    # Je recupere la liste des liens a scraper
    tableauLiens = pd.read_csv('C:/Users/lamine/PycharmProjects/dashboard1/dataset/lienVille.csv')
    listeLiens = tableauLiens['lien']

listeLiens = [lien for lien in listeLiens if str(lien)[:11] == '/management']


# print(listeLiens)


def parse(lien):
    dico = {
        **{i: '' for i in listeCles},
        **{"nbre allocataires (" + str(a) + ")": '' for a in range(2009, 2018)},
        **{"nbre RSA (" + str(a) + ")": '' for a in range(2009, 2018)},
        **{"nbre APL (" + str(a) + ")": '' for a in range(2009, 2018)},
        **{"nbre Alloc Familiales (" + str(a) + ")": '' for a in range(2009, 2018)},
    }
    dico['lien'] = lien
    dico['ville'] = tableauLiens[tableauLiens['lien'] == lien]['ville'].iloc[0]

    req = requests.get("http://www.journaldunet.com" + lien + "/sante-social")
    time.sleep(2)
    if req.status_code == 200:
        with open('C:/Users/lamine/PycharmProjects/dashboard1/dataset/santeSocial.csv', 'a',encoding = "ISO-8859-1") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=colonnes, lineterminator='\n')
            contenu = req.content
            soup = bs(contenu, "html.parser")

            tables = soup.findAll('table', class_="odTable odTableAuto")

            for i in range(len(tables)):
                infos = tables[i].findAll('tr')
                for info in infos[1:]:
                    cle = info.findAll('td')[0].text
                    valeur = info.findAll('td')[1].text

                    valeur = ''.join(valeur.split())
                    try:
                        dico[cle] = float(valeur)
                    except:
                        dico[cle] = valeur

            divs = soup.findAll('div', class_="hidden marB20")
            for div in divs:
                titre_h2 = div.find('h2')
                if titre_h2 != None and "Nombre d'allocataires" in titre_h2.text:
                    if div.find('script').string:
                        js_script = div.find('script').string
                        json_data = json.loads(js_script)
                        annees = json_data['xAxis']['categories']
                        donnees = json_data['series'][0]['data']

                        for annee, donnee in zip(annees, donnees):
                            try:
                                dico["nbre allocataires (" + str(annee) + ")"] = float(donnee)
                            except:
                                dico["nbre allocataires (" + str(annee) + ")"] = ''

                if titre_h2 != None and "Nombre de b�n�ficiaires du RSA" in titre_h2.text:
                    if div.find('script').string:
                        js_script = div.find('script').string
                        json_data = json.loads(js_script)
                        annees = json_data['xAxis']['categories']
                        donnees = json_data['series'][0]['data']

                        for annee, donnee in zip(annees, donnees):
                            try:
                                dico["nbre RSA (" + str(annee) + ")"] = float(donnee)
                            except:
                                dico["nbre RSA (" + str(annee) + ")"] = ''

                if titre_h2 != None and "l'aide au logement" in titre_h2.text:
                    if div.find('script').string:
                        js_script = div.find('script').string
                        json_data = json.loads(js_script)
                        annees = json_data['xAxis']['categories']
                        donnees = json_data['series'][0]['data']

                        for annee, donnee in zip(annees, donnees):
                            try:
                                dico["nbre APL (" + str(annee) + ")"] = float(donnee)
                            except:
                                dico["nbre APL (" + str(annee) + ")"] = ''

                if titre_h2 != None and "allocations familiales" in titre_h2.text:
                    if div.find('script').string:
                        js_script = div.find('script').string
                        json_data = json.loads(js_script)
                        annees = json_data['xAxis']['categories']
                        donnees = json_data['series'][0]['data']

                        for annee, donnee in zip(annees, donnees):
                            try:
                                dico["nbre Alloc Familiales (" + str(annee) + ")"] = float(donnee)
                            except:
                                dico["nbre Alloc Familiales (" + str(annee) + ")"] = ''

            writer.writerow(dico)
            print("[sante]", lien)



if __name__ == "__main__":
    with Pool(10) as p:
        p.map(parse, listeLiens)

