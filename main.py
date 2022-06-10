#!env python

import sqlite3
import argparse
from pathlib import Path
import requests


def get_urls(urlfile):
    """
    Reads lines from a file which contains API URLs generated
    by the servicodados.ibge.gov.br site. Each line should contain
    one URL.
    :param urlfile: Location of the file with URLs
    :return: A list of URLs
    """
    try:
        with open(urlfile, "r") as fd:
            urls = [line.strip() for line in fd]
    except IOError as e:
        raise Exception("I/O Error ({0}): {1}".format(e.errno, e.strerror))
    else:
        fd.close()

    return urls


def store_data(url_list, dbfile):
    """
    Parsing the results retrieved from IBGE API
    :param url_list: list of URLs to request,
        supports only one right now.
    :param dbfile: Location of db file
    :return: None
    """
    try:
        response = requests.get(url_list[0])
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

        query_schema = '''
        CREATE TABLE IF NOT EXISTS ibge_empresas (
            periodo INTEGER,
            numero_empresas INTEGER
        )
        '''
        self.db_cursor.execute(query_schema)
        self.db_con.commit()

        query_schema = '''
        CREATE TABLE IF NOT EXISTS ibge_pessoal (
            periodo INTEGER,
            pessoal_assalariado INTEGER
        )
        '''
        self.db_cursor.execute(query_schema)
        self.db_con.commit()

        query_schema = '''
        CREATE TABLE IF NOT EXISTS ibge_salario (            
            periodo INTEGER,
            salarios NUMERIC
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


def main():
    """
    Main function for running script and parsing arguments
    given to the script
    :return: None
    """
    parser = argparse.ArgumentParser(description='Process IBGE data.')
    parser.add_argument('--urlfile', dest='urlfile', action='store', required=True,
                        help='File containing urls for requests', default='ibge.db')
    parser.add_argument('--dbname', dest='dbname', action='store',
                        help='Name of SQLite file', default='ibge.db')

    args = parser.parse_args()

    if Path(args.urlfile).exists():
        urls = get_urls(Path(args.urlfile))
        store_data(urls, args.dbname)
    else:
        raise Exception(f"No File Found: {args.urlfile}")


if __name__ == '__main__':
    main()
