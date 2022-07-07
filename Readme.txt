PLEASE READ ME FIRST!

The ibge-lookup.py script requires two files, the URL and CNAE files.

The URL file is provided and the only thing that can be changed on that file is the
comma separated year section. DO NOT change anything else on the URL or the script will
no longer work. The IBGE API system will fail if you have too many years in the URL, keep it short!

https://servicodados.ibge.gov.br/.../periodos/2006,2007,2008,2009/..../[104029]|2703[117933]
                                             {^^THIS CAN CHANGE^^}

The CNAE file is also provided, but it can be modified. The longer the file is the more chances
there are of the IBGE system breaking down and not being able to serve the request due to its limitations.

If you run the script and for whatever reason you start getting HTTP 500 Errors, try reducing the
number of years and also the number of CNAEs you are trying to retrieve. Do it in paces.

The DBNAME file is optional. If you do not give a path/name it will create one called ibge.db within
your current directory. If the file already exists the script will only create the tables if they don't already
exist. It will also not add repeated entries (at least it shouldn't) since we are using INSERT OR REPLACE.

$ python3.9 ibge-lookup.py --help
usage: ibge-lookup.py [-h] --urlfile URLFILE --cnaefile CNAEFILE [--dbname DBNAME]

Process IBGE data.

optional arguments:
  -h, --help           show this help message and exit
  --urlfile URLFILE    File containing the url for the requests [Required]
  --cnaefile CNAEFILE  File containing CNAE IDs [Required]
  --dbname DBNAME      Name of SQLite file

