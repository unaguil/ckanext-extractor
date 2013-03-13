# -*- coding: utf8 -*- 

import os
import traceback
from logging import getLogger
from datetime import timedelta
from datetime import datetime

import uuid

from ckan.lib.celery_app import celery

import ConfigParser

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from model.transformation_model import Transformation
from extraction.extraction_context import ExtractionContext
from celery.task import periodic_task
from celery.schedules import crontab

from utils import get_instance, get_config_data

log = getLogger(__name__)

#Configuration load
config = ConfigParser.ConfigParser()
config.read(os.environ['CKAN_CONFIG'])

SQLALCHEMY_URL = config.get('app:main', 'sqlalchemy.url')
RUN_EVERY_SECONDS = 10

TRANSFORMATIONS_DIR = 'transformations'

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

@celery.task(name="extractor.install_dependencies")
def install_dependencies(required):
    log.info('Installing dependencies %s' % required)
    if len(required) > 0:
        required_str = ''
        for r in required:
            required_str += r + ' '

        os.system('pip install %s' % required_str)

def must_run(minute, hour, day_of_week):
    cron_tab = crontab(minute=minute, hour=hour, day_of_week=day_of_week)
    total_seconds = cron_tab.remaining_estimate(datetime.now()).total_seconds()
    return total_seconds < RUN_EVERY_SECONDS

@periodic_task(run_every=timedelta(seconds=int(RUN_EVERY_SECONDS)))
def launch_transformations():
    log.info('Checking transformation crontabs')
    engine = create_engine(SQLALCHEMY_URL, convert_unicode=True, pool_recycle=3600)
    session = sessionmaker(bind = engine)()

    transformations = session.query(Transformation).all()

    session.close()

    for t in transformations:
        if must_run(t.minute, t.hour, t.day_of_week):
            mainclass, _ = get_config_data(t.output_dir)
            celery.send_task("extractor.perform_extraction",
                args=[t.package_id, mainclass], task_id=str(uuid.uuid4()))
