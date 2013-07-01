ckanext-extractor
=================

Plugin for automatic extraction of data sources.

Tested with CKAN 1.8

 Installation
--------------

**Install plugin**

    python setup.py install
    
**Update CKAN development.ini file to load the plugin**

    ckan.plugins = stats extractor
    
**Initialize new tables on CKAN database (Change user & pass)**

    python ckanext/extractor/model/initDB.py
