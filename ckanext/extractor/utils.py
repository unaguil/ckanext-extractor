import sys
import os
import ConfigParser
import ast

SETUP_FILE = 'manifest.mf'

def my_import(name):
        module, clazz = name.split(':')
        mod = __import__(module, fromlist = [''])
        reload(mod)
        return getattr(mod, clazz)

def get_instance(transformation_dir, mainclass):
    sys.path.append(transformation_dir)
    #import main class and instantiate transformation
    clazz = my_import(mainclass)
    transformation_instance = clazz()
    sys.path.remove(transformation_dir)
    return transformation_instance

def get_config_data(transformation_dir):
    os.chdir(transformation_dir)
    config = ConfigParser.ConfigParser()
    config.readfp(open(SETUP_FILE))
    mainclass = config.get('ckan-extractor', 'mainclass')
    install_requires = ast.literal_eval(config.get('ckan-extractor', 'install_requires'))
    return mainclass, install_requires
