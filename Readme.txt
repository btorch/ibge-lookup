PLEASE READ ME FIRST!

This script was created to retrieve information from table 992 for 5 specific "variaves".

- 2585 Numero de empresas e outras organizacoes
- 3875 Pessoal ocupado assalariado com nivel superior completo
- 3876 Pessoal ocupado assalariado sem nivel superior completo
- 3879 Salarios e outras remuneracoes dos empregados com nivel superior completo
- 3880 Salarios e outras remuneracoes dos empregados sem nivel superior completo

It can be modified to retrieve other information but it's main purpose was for the above only.

The ibge-lookup.py script requires two files, the URL and CNAE files.

The URL file is provided and the only thing that can be changed on that file is the
comma separated year section. DO NOT change anything else on the URL or the script will
no longer work unles modifications are done to the script to reflect the changes. The
IBGE API system will fail if you have too many years in the URL, keep it short!

https://servicodados.ibge.gov.br/.../periodos/2006,2007,2008,2009/..../[104029]|2703[117933]
                                             {^^THIS CAN CHANGE^^}

The CNAE file is also provided, but it can be modified. The longer the file is the more chances
there are of the IBGE system breaking down and not being able to serve the request due to its limitations.

If you run the script and for whatever reason you start getting HTTP 500 Errors, try reducing the
number of years and also the number of CNAEs you are trying to retrieve. Do it in paces.

The DBNAME file is optional. If you do not give a path/name it will create one called ibge.db within
your current directory. If the file already exists the script will only create the tables if they don't already
exist. It will also not add repeated entries since we are using INSERT OR REPLACE and also have UNIQUE constraints.

Configuration file:
Niveis Geograficos permitidos
O Nivel N2 pode ter  1 ou mais separados por virgulas
O Nivel N3 pode ser todos os estados como abaixo ou apenas alguns
Nivel = N1 com Localidade all (Brasil inteiro)
Nivel = N2 com localidades 1 para Norte, 2 para Nordeste, 3 para Sudeste, 4 para Sul, 5 para Centro-Oeste
Nivel = N3 com localidades {"Rondonia":11,"Acre":12,"Amazonas":13,"Roraima":14,"Para":15,
                               "Amapa":16,"Tocantins":17,"Maranhao":21,"Piaui":22,"Ceara":23,
                               "Rio Grande do Norte":24,"Paraiba":25,"Pernambuco":26,"Alagoas":27,
                               "Sergipe":28,"Bahia":29,"Minas Gerais":31,"Espirito Santo":32,
                               "Rio de Janeiro":33,"Sao Paulo":35,"Parana":41,"Santa Catarina":42,
                               "Rio Grande do Sul":43,"Mato Grosso do Sul":50,"Mato Grosso":51,
                               "Goias":52,"Distrito Federal":53}
Exemplo Brasil:
[geo]
nivel = N1
localidades = all

Exemplo Regioes:
[geo]
nivel = N2
localidades = 1,2

Exemplo Estados:
[geo]
nivel = N3
localidades = {"Rondonia":11,"Acre":12,"Amazonas":13,"Roraima":14}


$ python3.9 ibge-lookup.py --help
usage: ibge-lookup.py [-h] --conf CONFIGFILE --urlfile URLFILE --cnaefile CNAEFILE [--dbname DBNAME]

Process IBGE data.

optional arguments:
  -h, --help           show this help message and exit
  --conf CONFIGGILE    Cofiguration file to be read
  --urlfile URLFILE    File containing the url for the requests [Required]
  --cnaefile CNAEFILE  File containing CNAE IDs [Required]
  --dbname DBNAME      Name of SQLite file

