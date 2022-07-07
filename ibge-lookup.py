#!/usr/bin/python3

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
# Compatibility: Python 3.6 and beyond, recommended Python 3.9
#

import sqlite3
import argparse
from pathlib import Path
import requests
import sys


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
    :return: None
    """
    special_data = ['...', '..', 'X', '-']
    for estado in estados.keys():
        temp_url = url.replace("LOCATION", str(estados[estado]))
        x = 0
        y = 50
        main_holder = []
        while x <= len(cnaes):
            new_url = temp_url.replace("[CNAE]", str(cnaes[x:y]).replace(" ", ""))
            x += 50
            y += 50
            try:
                response = requests.get(new_url)
                response.raise_for_status()
                ibge_data = response.json()
                for ibge in ibge_data:
                    for resultado in ibge['resultados']:
                        cnae_id, cnae_txt = next(iter(resultado['classificacoes'][0]['categoria'].items()))
                        for serie in resultado['series']:
                            for year, data in serie['serie'].items():
                                if data in special_data:
                                    data = '0'
                                main_holder.append([ibge['id'], estado, cnae_id, cnae_txt, year, data])

            except requests.exceptions.HTTPError as errh:
                print("Http Error:", errh)
            except requests.exceptions.ConnectionError as errc:
                print("Error Connecting:", errc)
            except requests.exceptions.Timeout as errt:
                print("Timeout Error:", errt)
            except requests.exceptions.RequestException as err:
                print("OOps: Something Else", err)
        store_data(db, main_holder)
        print(f'Data added into DB for {estado}')
        '''A break for testing
        if estado == 'Acre':
            break
        '''


def store_data(db, data_list):
    """
    Receives a list which will contain all IBGE data retrieved
    for a state, properly format it and then use the db Object
    to add the data into the proper table.
    :param db: Sqlite DB Object
    :param data_list: List containing IBGE data
    :return: None
    """
    for data in data_list:
        table_name = tables[data[0]]
        table_values = tuple(data[1:])
        db.add_data(table_name, table_values)


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
        2585 Numero de empresas e outras organizacoes
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
        3875 Pessoal ocupado assalariado com nivel superior completo
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
        3876 Pessoal ocupado assalariado sem nivel superior completo
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
        3879 Salarios e outras remuneracoes dos empregados com nivel superior completo
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
        3880 Salarios e outras remuneracoes dos empregados sem nivel superior completo
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
        query = f"INSERT OR REPLACE INTO {table} VALUES (?,?,?,?,?)"
        try:
            self.db_cursor.execute(query, data)
            self.db_con.commit()
        except sqlite3.Error as e:
            print("An error occurred:", e.args[0])

    def db_close(self):
        """
        Just to close the DB connection
        :return: None
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
    :return: 0 in a successful completion
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

    db = DbBackend(Path(args.dbname))
    collect_data(db, raw_url, raw_cnaes)
    db.db_close()

    return 0


if __name__ == '__main__':
    status = main()
    sys.exit(status)
