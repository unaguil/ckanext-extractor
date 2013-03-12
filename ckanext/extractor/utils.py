import sys
import os
import ConfigParser

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

def get_main_class(transformation_dir):
    os.chdir(transformation_dir)
    config = ConfigParser.ConfigParser()
    config.readfp(open('entry_point.txt'))
    return config.get('ckan-extractor', 'mainclass')