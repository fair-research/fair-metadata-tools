# Fair Metadata Tools

Prerequisites
-------------

Download and install the Globus Search client <https://github.com/globusonline/search-client>. 
``make install`` creates a virtual environment in ``.venv/`` subdirectory, installs globus-sdk 
and all necessary Python modules the search-client depends on, so there is no need to set up 
a virtual environment prior installing the search-client. The search-client also takes care 
of getting an OAuth2 access token with an appropriate scope for the Globus Search. 


Generate GIngest records
------------------------

To generate files with GIngest records, run

``python gen_ingest.py``

The script will generate one ``GMetaList`` per file with 100 ``GIngest`` records in each in ``gingest/`` subdirectory. 
(The script generates a separate file for 100 ``GIngest`` records to avoid ``ConnectionError`` the requests module may raise 
when a large file is sent in the POST request, <https://github.com/requests/requests/issues/2422>). 

You can print the records using ``cat`` and Python json tool, for example:

``cat gingest/GTEx_v7_gingest_0_100.json | python -mjson.tool``

Ingest metadata
---------------

To ingest all metadata to a search index, run

`` search-client --index <UUID_of_the_index> ingest gingest/*``

Search
------

To query the index, run for example:

``search-client --index <UUID_of_the_index> query '{"q":"never smoker"}'``
