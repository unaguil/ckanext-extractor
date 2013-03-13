# -*- coding: utf8 -*- 

from ckan.lib.base import render, c, model, response
from ckan.logic import get_action
from logging import getLogger
from ckan.controllers.package import PackageController
from pylons import request

import sys
import os
import os.path
import shutil
import ConfigParser
from zipfile import ZipFile
from datetime import datetime

from model.transformation_model import Transformation, Extraction
from extraction.extraction_context import WORKING

import uuid
from ckan.lib.celery_app import celery

from utils import get_main_class, get_instance

log = getLogger(__name__)

TRANSFORMATIONS_DIR = 'transformations'

class ExtractorController(PackageController):

    def get_transformations_dir(self):
        here = os.path.dirname(__file__)
        rootdir = os.path.dirname(os.path.dirname(here))
        return os.path.join(rootdir, TRANSFORMATIONS_DIR)

    def get_transformation_data(self, id, data):
        transformation = model.Session.query(Transformation).filter_by(package_id=id).first()

        if transformation is not None:
            data.timestamp = transformation.timestamp.isoformat()
            data.filename = transformation.filename
            data.enabled = transformation.enabled
            data.extractions = transformation.extractions
            data.hour = transformation.hour
            data.minute = transformation.minute
            data.day_of_week = transformation.day_of_week
            data.data = True
        else:
            data.data = False
            data.minute = '59'
            data.hour = '23'
            data.day_of_week = '*'
        
    def show_extractor_config(self, id):
        log.info('Showing extractor configuration for package name: %s' % id)         

        # using default functionality
        self.read(id)

        context = {'model': model, 'session': model.Session, 'user': c.user or c.author}
        package_info = get_action('package_show')(context, {'id': c.pkg.id})

        self.get_transformation_data(package_info['id'], c)

        c.error = False

        #rendering using template
        return render('extractor/read.html')

    def render_error_messsage(self, message):
        c.error = True
        c.error_message = message
        return render('extractor/read.html')

    def extract_zip_file(self, transformation, submitted_file):
        log.info('Extracting zip file %s' % transformation.filename)

        #create transformations directory if it does not exist
        transformations_dir = self.get_transformations_dir()
        if not os.path.isdir(transformations_dir):
            log.info('Creating transformations directory')
            os.mkdir(transformations_dir)

        #delete package directory if it exists
        package_dir = os.path.join(transformations_dir, transformation.package_id)
        if os.path.isdir(package_dir):
            log.info('Deleting package directory %s' % package_dir)
            shutil.rmtree(package_dir)

        #create directory again and change to it
        log.info('Creating package directory %s' % package_dir)
        os.mkdir(package_dir)
        os.chdir(package_dir)

        zipfile = ZipFile(submitted_file)
        log.info('Extracting file %s to directory %s' % (transformation.filename, package_dir))
        zipfile.extractall()

        #obtain data
        submitted_file.seek(0)
        transformation.data = submitted_file.read()
        transformation.timestamp = datetime.now()

        log.info('File %s extracted' % transformation.filename)
        return package_dir

    def submit_transformation(self, id):
        log.info('Processing submitted transformation for package name: %s' % id)

         # using default functionality
        self.read(id)

        context = {'model': model, 'session': model.Session, 'user': c.user or c.author}
        package_info = get_action('package_show')(context, {'id': c.pkg.id})

        transformation = model.Session.query(Transformation).filter_by(package_id=package_info['id']).first()
        if transformation is None:
            transformation = Transformation(package_info['id'])
        else:
            #read enabled status of transformation
            transformation.minute = request.params['minute']
            transformation.hour = request.params['hour']
            transformation.day_of_week = request.params['day_of_week']
            transformation.enabled = 'enabled' in request.params
            
        #read submitted file
        if 'transformation_code' in request.params and request.params['transformation_code'] is not u'':
            try:
                transformation.filename = request.params['transformation_code'].filename
                submitted_file = request.params['transformation_code'].file

                transformation.output_dir = self.extract_zip_file(transformation, submitted_file)
                self.deploy_transformation(transformation)
            except Exception as e:
                return self.render_error_messsage('Problem deploying uploaded file %s (%s)' % (transformation.filename, e))

        model.Session.merge(transformation)
        model.Session.commit()
        log.info("Transformation object stored for package name '%s'" % id)

        #rendering using default template
        return render('package/read.html')

    def deploy_transformation(self, transformation):
        transformation_instance = get_instance(transformation.output_dir, get_main_class(transformation.output_dir))
        transformation_instance.create_db()

        #remove extraction log
        transformation.extractions = []
        model.Session.merge(transformation)
        model.Session.commit()

    def perform_deploy(self, id):
        log.info('Deploying transformation for package name: %s' % id)

        # using default functionality
        self.read(id)

        transformation = model.Session.query(Transformation).filter_by(package_id=c.pkg.id).first()
        if transformation:
            self.deploy_transformation(transformation)

        #rendering using default template
        return render('package/read.html')

    def download_transformation(self, id):
        log.info('Dowloading transformation for package name: %s' % id)

        # using default functionality
        self.read(id)

        context = {'model': model, 'session': model.Session, 'user': c.user or c.author}
        package_info = get_action('package_show')(context, {'id': c.pkg.id})

        transformation = model.Session.query(Transformation).filter_by(package_id=c.pkg.id).first()
        if transformation is not None:
            response.status_int = 200
            response.headers['Content-Type'] = 'application/octet-stream'
            response.headers['Content-Disposition'] = 'attachment; filename="%s"' % transformation.filename
            response.headers['Content-Length'] = len(transformation.data)
            response.headers['Cache-Control'] = 'no-cache, must-revalidate'
            response.headers['Pragma'] = 'no-cache'

            return transformation.data

    def launch_transformation(self, id):
        log.info('Launching transformation for package name: %s' % id)

        # using default functionality
        self.read(id)

        context = {'model': model, 'session': model.Session, 'user': c.user or c.author}
        package_info = get_action('package_show')(context, {'id': c.pkg.id})

        t = model.Session.query(Transformation).filter_by(package_id=package_info['id']).first()

        celery.send_task("extractor.perform_extraction",
            args=[t.package_id, get_main_class(t.output_dir)], task_id=str(uuid.uuid4()))

        self.get_transformation_data(package_info['id'], c)
        c.error = False

        #rendering using template
        return render('extractor/read.html')

    def show_message(self, id, extraction_id):
        log.info('Showing message for extraction %s of package name: %s ' % (extraction_id, id))

        # using default functionality
        self.read(id)

        context = {'model': model, 'session': model.Session, 'user': c.user or c.author}
        package_info = get_action('package_show')(context, {'id': c.pkg.id})

        extraction = model.Session.query(Extraction).filter_by(transformation_id=package_info['id'], id=extraction_id).first()

        if extraction.transformation_status == WORKING:
            c.comment = 'Context: %s' % extraction.context
        else:
            c.comment = extraction.comment

        #render template
        return render('extractor/comment.html')
        