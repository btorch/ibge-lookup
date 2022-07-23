PLEASE READ ME FIRST!

This script was initally created to retrieve information from table 992 for 5 specific "variaves" as shown below.
- 2585 Numero de empresas e outras organizacoes
- 3875 Pessoal ocupado assalariado com nivel superior completo
- 3876 Pessoal ocupado assalariado sem nivel superior completo
- 3879 Salarios e outras remuneracoes dos empregados com nivel superior completo
- 3880 Salarios e outras remuneracoes dos empregados sem nivel superior completo

It has been updated in order to allow information to be retrieved from other tables, but
it has not been tested yet. One will likely also need to update the database table setup
within the code in order to create proper tables for the new values being retrieved.

The ibge-lookup.py script requires now requires a configuration file. A default
configuration file is provided and can be modified. One can set the flag --conf in order
to point to the desired config file.

Configuration File Explained:

[ibge]
This section of the configuration file will hold three values:
- agregado: which is the ibge table number
- periodos: which are the years one want to collect separated by comma
- variaveis: which are the 5 ibge variaveis one wants to collect. No more than 5.

Please note #1: Try to set only 3-4 years at a time otherwise the IBGE api may give 500 errors.
Please note #2: If one changes the variaveis, one will likely have to change the database schema within the code.


[cnaes]
This section has one key called cnae_arquivo which points to a file that has the cnaes IDs one per line.

The CNAE file is also provided, but it can be modified. The longer the file is the more chances
there are of the IBGE system breaking down and not being able to serve the request due to its limitations.

If you run the script and for whatever reason you start getting HTTP 500 Errors, try reducing the
number of years and also the number of CNAEs you are trying to retrieve. Do it in paces.


[geo]
This section has two keys, nivel and localidades.
When using N1, N2 or N3, one can set an empty value {} or one or more key:value pairs
Nivel = N1 with Localidades {"Brasil": "all"} or {}
Nivel = N2 with localidades {"Norte": 1, "Nordeste": 2, "Sudeste": 3, "Sul": 4, "Centro-Oeste": 5 }
Nivel = N3 with localidades {"Rondonia":11, "Acre":12, "Amazonas":13, "Roraima":14, "Para":15,
                               "Amapa":16, "Tocantins":17, "Maranhao":21, "Piaui":22, "Ceara":23,
                               "Rio Grande do Norte":24, "Paraiba":25, "Pernambuco":26, "Alagoas":27,
                               "Sergipe":28, "Bahia":29, "Minas Gerais":31, "Espirito Santo":32,
                               "Rio de Janeiro":33, "Sao Paulo":35, "Parana":41, "Santa Catarina":42,
                               "Rio Grande do Sul":43, "Mato Grosso do Sul":50, "Mato Grosso":51,
                               "Goias":52, "Distrito Federal":53}
E.g Brasil:
[geo]
nivel = N1
localidades = {"Brasil": "all"}

E.g Regioes:
[geo]
nivel = N2
localidades = {"Norte": 1, "Nordeste": 2}

E.g Estados:
[geo]
nivel = N3
localidades = {"Rondonia": 11,"Acre":12,"Amazonas": 13,"Roraima": 14}

-- End Configuration Explanation --


Database Setup:

The --dbname flag is used to provide a name for the sqlite database file. The default file name is set to
ibge.db. If the file already exists the script will only create the tables if they don't already
exist. It will also not add repeated entries since we are using INSERT OR REPLACE and
also have UNIQUE constraints.


$ python3.9 ibge-lookup.py --help
usage: ibge-lookup.py [-h] --conf CONFIGFILE --urlfile URLFILE [--dbname DBNAME]

Process IBGE data.

optional arguments:
  -h, --help           show this help message and exit
  --conf CONFIGGILE    Cofiguration file to be read
  --dbname DBNAME      Name of SQLite file

