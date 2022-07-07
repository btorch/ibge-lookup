#!/usr/bin/python

# Copyright (c) 2022 Marcelo Martins
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied. See the License for the specific language governing permissions and
# limitations under the License.
#
# Author: Marcelo Martins
# Email: btorch@zeroaccess.org
# Created: June, 2022
# Contributors: None
# Compatibility: Python 3.6 and beyond
#

import sqlite3
import argparse
from pathlib import Path
import requests


estados = {
    "Rondonia": 11,
    "Acre": 12,
    "Amazonas": 13,
    "Roraima": 14,
    "Para": 15,
    "Amapa": 16,
    "Tocantins": 17,
    "Maranhao": 21,
    "Piaui": 22,
    "Ceara": 23,
    "Rio Grande do Norte": 24,
    "Paraiba": 25,
    "Pernambuco": 26,
    "Alagoas": 27,
    "Sergipe": 28,
    "Bahia": 29,
    "Minas Gerais": 31,
    "Espirito Santo": 32,
    "Rio de Janeiro": 33,
    "Sao Paulo": 35,
    "Parana": 41,
    "Santa Catarina": 42,
    "Rio Grande do Sul": 43,
    "Mato Grosso do Sul": 50,
    "Mato Grosso": 51,
    "Goias": 52,
    "Distrito Federal": 53
}

tables = {
    '2585': 'ibge_empresas',
    '3875': 'ibge_pessoal_cns',
    '3876': 'ibge_pessoal_sns',
    '3879': 'ibge_salario_cns',
    '3880': 'ibge_salario_sns'
}

def get_url(urlfile):
    """
    Read the first line from the file which contains the API URL generated
    by the servicodados.ibge.gov.br site. If you would like more periods (years)
    please change the URL to contain the years separated by comma.
    Do not request more than 3-4 years at a time.
    :param urlfile: Location of the file with the URL
    :return: The URL to be used
    """
    try:
        with open(urlfile, "r") as fd:
            "url = [line.strip() for line in fd]"
            url = fd.readline().strip()
            raw_url = url.replace("N3[all]", "N3[LOCATION]").replace("12762[all]", "12762[CNAE]")
    except IOError as e:
        raise Exception("I/O Error ({0}): {1}".format(e.errno, e.strerror))
    else:
        fd.close()

    return raw_url


def get_cnaes(cnaefile):
    """
    Read all CNAES from the CNAE file and places them into
    a list for later use.
    :param cnaefile: Location of the file with the CNAEs
    :return: List of CNAEs
    """
    try:
        with open(cnaefile, "r") as fd:
            cnaes = [int(line.strip()) for line in fd]
    except IOError as e:
        raise Exception("I/O Error ({0}): {1}".format(e.errno, e.strerror))
    else:
        fd.close()

    return cnaes


def collect_data(db, url, cnaes):
    """
    Retrieve data from IBGE API endpoint, add it to a
    list holder. Pass the list over to the store function
    on each state pass to be inserted into table.
    :param db: Database Object
    :param url: URL Endpoint
    :param cnaes: List of CNAEs
    :return:
    """
    for estado in estados.keys():
        temp_url = url.replace("LOCATION", str(estados[estado]))
        x = 0
        y = 50
        main_holder = []
        while x <= len(cnaes):
            new_url = temp_url.replace("[CNAE]",str(cnaes[x:y]).replace(" ",""))
            x += 50
            y += 50
            try:
                response = requests.get(new_url)
                response.raise_for_status()
                ibge_data = response.json()
                for ibge in ibge_data:
                    #tb_name = tables[ibge['id']]
                    for resultado in ibge['resultados']:
                        cnae_id, cnae_txt = next(iter(resultado['classificacoes'][0]['categoria'].items()))
                        for serie in resultado['series']:
                            for year, data in serie['serie'].items():
                                if data == '...':
                                    data = '0'
                                main_holder.append([ibge['id'],estado,cnae_id,cnae_txt,year,data])

            except requests.exceptions.HTTPError as errh:
                print ("Http Error:",errh)
            except requests.exceptions.ConnectionError as errc:
                print("Error Connecting:", errc)
            except requests.exceptions.Timeout as errt:
                print("Timeout Error:", errt)
            except requests.exceptions.RequestException as err:
                print("OOps: Something Else", err)

        print(main_holder)
        store_data(db, main_holder)





def store_data(db, data):
    """
    Parsing the results retrieved from IBGE API
    :param url_list: list of URLs to request,
        supports only one right now.
    :param dbfile: Location of db file
    :return: None
    """
    try:
        response = requests.get(url)
        response.raise_for_status()

        ibge_data = response.json()
        empresas_data = ibge_data[0]['resultados'][0]['series'][0]['serie']
        pessoal_data = ibge_data[1]['resultados'][0]['series'][0]['serie']
        salarios_data = ibge_data[2]['resultados'][0]['series'][0]['serie']

        db = DbBackend(dbfile)
        db.add_data('ibge_empresas', empresas_data)
        db.add_data('ibge_pessoal', pessoal_data)
        db.add_data('ibge_salario', salarios_data)

    except requests.exceptions.HTTPError as e:
        print(e)


class DbBackend:
    def __init__(self, dbname):
        """
        Initialise the SQLite connection and
        create a db schema if one doesn't exist yet
        :param dbname: Argument passed over identifying
            the sqlite3 file
        """
        dbfile = Path(dbname)
        self.db_con = sqlite3.connect(dbfile)
        self.db_cursor = self.db_con.cursor()

        """
        Table ibge_empresas for variable:
        2585 Número de empresas e outras organizações
        """
        query_schema = '''
        CREATE TABLE IF NOT EXISTS ibge_empresas (
            localidade TEXT,
            cnae_id INTEGER,
            cnae TEXT,
            periodo INTEGER,
            numero_empresas INTEGER
        )
        '''
        self.db_cursor.execute(query_schema)
        self.db_con.commit()

        """
        Table ibge_pessoal_cns for variable:
        3875 Pessoal ocupado assalariado com nível superior completo
        """
        query_schema = '''
        CREATE TABLE IF NOT EXISTS ibge_pessoal_cns (
            localidade TEXT,
            cnae_id INTEGER,
            cnae TEXT,   
            periodo INTEGER,
            pessoal_assalariado_cns INTEGER
        )
        '''
        self.db_cursor.execute(query_schema)
        self.db_con.commit()

        """
        Table ibge_pessoal_sns for variable:
        3876 Pessoal ocupado assalariado sem nível superior completo
        """
        query_schema = '''
        CREATE TABLE IF NOT EXISTS ibge_pessoal_sns (
            localidade TEXT,
            cnae_id INTEGER,
            cnae TEXT,
            periodo INTEGER,
            pessoal_assalariado_sns INTEGER
        )
        '''
        self.db_cursor.execute(query_schema)
        self.db_con.commit()

        """
        Table ibge_salario_cns for variable:
        3879 Salários e outras remunerações dos empregados com nível superior completo
        """
        query_schema = '''
        CREATE TABLE IF NOT EXISTS ibge_salario_cns (
            localidade TEXT,        
            cnae_id INTEGER,
            cnae TEXT,                 
            periodo INTEGER,
            valor_salarial_cns NUMERIC
        )
        '''
        self.db_cursor.execute(query_schema)
        self.db_con.commit()

        """
        Table ibge_salario_sns for variable:
        3880 Salários e outras remunerações dos empregados sem nível superior completo
        """
        query_schema = '''
        CREATE TABLE IF NOT EXISTS ibge_salario_sns (
            localidade TEXT,        
            cnae_id INTEGER,
            cnae TEXT,                 
            periodo INTEGER,
            valor_salarial_sns NUMERIC
        )
        '''
        self.db_cursor.execute(query_schema)
        self.db_con.commit()


    def add_data(self, table, data):
        """
        Inserts data retrieved by url request into db
        :param table: Name of the table to add entries into
        :param data: Values to be added to table
        :return: None
        """
        for item in data.items():
            query = f"INSERT OR REPLACE INTO {table} VALUES {item}"
            self.db_cursor.execute(query)

        self.db_con.commit()

    def db_close(self):
        """
        Just to close the DB connection
        :return: Nothing to return
        """
        try:
            self.db_con.commit()
            self.db_cursor.close()
            self.db_con.close()
        except sqlite3.Error as e:
            print("An error occurred:", e.args[0])



def main():
    """
    Main function for running script and parsing arguments
    given to the script
    :return: None
    """
    parser = argparse.ArgumentParser(description='Process IBGE data.')
    parser.add_argument('--urlfile', dest='urlfile', action='store', required=True,
                        help='File containing the url for the requests', default='url.txt')
    parser.add_argument('--cnaefile', dest='cnaefile', action='store', required=True,
                        help='File containing CNAE IDs', default='cnae.txt')
    parser.add_argument('--dbname', dest='dbname', action='store',
                        help='Name of SQLite file', default='ibge.db')

    args = parser.parse_args()

    if Path(args.urlfile).exists():
        raw_url = get_url(Path(args.urlfile))
    else:
        raise Exception(f"No File Found: {args.urlfile}")

    if Path(args.cnaefile).exists():
        raw_cnaes = get_cnaes(Path(args.cnaefile))
    else:
        raise Exception(f"No File Found: {args.cnaefile}")

    if Path(args.dbname).exists():
        db = DbBackend(Path(args.dbname))
    else:
        raise Exception(f"No File Found: {args.dbname}")

    collect_data(db, raw_url, raw_cnaes)


if __name__ == '__main__':
    main()
