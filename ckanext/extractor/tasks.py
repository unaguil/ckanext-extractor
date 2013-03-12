# -*- coding: utf8 -*- 

import sys
import os
import traceback
from logging import getLogger

from logging import getLogger
from ckan.lib.celery_app import celery

import ConfigParser

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from model.transformation_model import Transformation
from extraction.extraction_context import ExtractionContext

log = getLogger(__name__)

#Configuration load
config = ConfigParser.ConfigParser()
config.read(os.environ['CKAN_CONFIG'])

SQLALCHEMY_URL = config.get('app:main', 'sqlalchemy.url')

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

@celery.task(name="extractor.perform_extraction")
def perform_extraction(transformation_dir, package_id, mainclass):
    engine = create_engine(SQLALCHEMY_URL, convert_unicode=True, pool_recycle=3600)
    session = sessionmaker(bind = engine)()

    #change to transformation directory
    os.chdir(transformation_dir)

    transformation = session.query(Transformation).filter_by(package_id=package_id).first()

    #create context and call transformation entry point
    context = ExtractionContext(transformation, session)

    log.info('Starting transformation %s' % package_id)
    transformation_instance = get_instance(transformation_dir, mainclass)

    try: 
        transformation_instance.start_transformation(context)
    except:
        comment = traceback.format_exc()
        context.finish_error(comment)
        log.info(comment)

    session.close()