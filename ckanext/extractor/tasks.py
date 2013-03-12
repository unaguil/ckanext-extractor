# -*- coding: utf8 -*- 

import sys
import os
import traceback
from logging import getLogger
from datetime import timedelta

import uuid

from ckan.lib.celery_app import celery

import ConfigParser

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from model.transformation_model import Transformation
from extraction.extraction_context import ExtractionContext
from celery.task.control import inspect
from celery.task import periodic_task

log = getLogger(__name__)

#Configuration load
config = ConfigParser.ConfigParser()
config.read(os.environ['CKAN_CONFIG'])

SQLALCHEMY_URL = config.get('app:main', 'sqlalchemy.url')
RUN_EVERY = 10

TRANSFORMATIONS_DIR = 'transformations'

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

@celery.task(name="extractor.perform_extraction")
def perform_extraction(package_id, mainclass):
    engine = create_engine(SQLALCHEMY_URL, convert_unicode=True, pool_recycle=3600)
    session = sessionmaker(bind = engine)()

    t = session.query(Transformation).filter_by(package_id=package_id).first()

    #change to transformation directory
    os.chdir(t.output_dir)

    #create context and call transformation entry point
    context = ExtractionContext(t, session)

    log.info('Starting transformation %s' % package_id)
    transformation_instance = get_instance(t.output_dir, mainclass)

    try: 
        transformation_instance.start_transformation(context)
    except:
        comment = traceback.format_exc()
        context.finish_error(comment)
        log.info(comment)

    session.close()

@periodic_task(run_every=timedelta(seconds=int(RUN_EVERY)))
def launch_transformations():
    log.info('Running periodic task')
    engine = create_engine(SQLALCHEMY_URL, convert_unicode=True, pool_recycle=3600)
    session = sessionmaker(bind = engine)()

    transformations = session.query(Transformation).all()

    session.close()
    
    for t in transformations:
        celery.send_task("extractor.perform_extraction", args=[t.package_id, get_main_class(t.output_dir)], task_id=str(uuid.uuid4()))
