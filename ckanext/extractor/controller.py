# -*- coding: utf8 -*- 

from ckan.lib.base import render, c, model, response
from ckan.logic import get_action
from logging import getLogger
from ckan.controllers.package import PackageController
from pylons import request

import sys
import traceback
import os
import os.path
import shutil
from zipfile import ZipFile
from datetime import datetime

from model.transformation_model import Transformation, Extraction
from extraction.extraction_context import ExtractionContext

log = getLogger(__name__)

TRANSFORMATIONS_DIR = 'transformations'

class ExtractorController(PackageController):

    def get_transformations_dir(self):
        here = os.path.dirname(__file__)
        rootdir = os.path.dirname(os.path.dirname(here))
        return os.path.join(rootdir, TRANSFORMATIONS_DIR)

    def get_transformation_data(self, id):
        transformation = model.Session.query(Transformation).filter_by(package_id=id).first()

        if transformation is not None:
            c.timestamp = transformation.timestamp.isoformat()
            c.mainclass = transformation.mainclass
            c.filename = transformation.filename
            c.enabled = transformation.enabled
            c.extractions = transformation.extractions
            c.data = True
        else:
            c.data = False

        return c
        
    def show_extractor_config(self, id):
        log.info('Showing extractor configuration for package name: %s' % id)         

        # using default functionality
        template = self.read(id)

        context = {'model': model, 'session': model.Session, 'user': c.user or c.author}
        package_info = get_action('package_show')(context, {'id': c.pkg.id})

        self.get_transformation_data(package_info['id'])

        c.error = False

        #rendering using default template
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

    def extract_transformation(self, id):
        log.info('Processing submitted transformation for package name: %s' % id)

         # using default functionality
        template = self.read(id)

        context = {'model': model, 'session': model.Session, 'user': c.user or c.author}
        package_info = get_action('package_show')(context, {'id': c.pkg.id})

        transformation = model.Session.query(Transformation).filter_by(package_id=package_info['id']).first()
        if transformation is None:
            transformation = Transformation(package_info['id'])
        else:
            #read enabled status of transformation
            transformation.enabled = 'enabled' in request.params
            
        #get mainclass value
        if 'mainclass' in request.params:
            transformation.mainclass = request.params['mainclass']

        #read submitted file
        if 'transformation_code' in request.params and request.params['transformation_code'] is not u'':
            try:
                transformation.filename = request.params['transformation_code'].filename
                submitted_file = request.params['transformation_code'].file

                transformation_dir = self.extract_zip_file(transformation, submitted_file)

                transformation_instance = self.get_instance(transformation_dir, transformation.mainclass)
                transformation_instance.create_db()
            except Exception as e:
                return self.render_error_messsage('Problem deploying uploaded file %s (%s)' % (transformation.filename, e))

        model.Session.merge(transformation)
        model.Session.commit()
        log.info("Transformation object stored for package name '%s'" % id)

        #rendering using default template
        return render('package/read.html')

    def download_transformation(self, id):
        log.info('Dowloading transformation for package name: %s' % id)

        # using default functionality
        template = self.read(id)

        context = {'model': model, 'session': model.Session, 'user': c.user or c.author}
        package_info = get_action('package_show')(context, {'id': c.pkg.id})

        transformation = model.Session.query(Transformation).filter_by(package_id=package_info['id']).first()
        if transformation is not None:
            response.status_int = 200
            response.headers['Content-Type'] = 'application/octet-stream'
            response.headers['Content-Disposition'] = 'attachment; filename="%s"' % transformation.filename
            response.headers['Content-Length'] = len(transformation.data)
            response.headers['Cache-Control'] = 'no-cache, must-revalidate'
            response.headers['Pragma'] = 'no-cache'

            return transformation.data

    def my_import(self, name):
        module, clazz = name.split(':')
        mod = __import__(module, fromlist = [''])
        reload(mod)
        return getattr(mod, clazz)

    def get_instance(self, transformation_dir, mainclass):
        sys.path.append(transformation_dir)
        #import main class and instantiate transformation
        clazz = self.my_import(mainclass)
        transformation_instance = clazz()
        sys.path.remove(transformation_dir)
        return transformation_instance

    def launch_transformation(self, id):
        log.info('Launching transformation for package name: %s' % id)

        # using default functionality
        template = self.read(id)

        context = {'model': model, 'session': model.Session, 'user': c.user or c.author}
        package_info = get_action('package_show')(context, {'id': c.pkg.id})

        #change to transformation directory
        transformation_dir = os.path.join(self.get_transformations_dir(), package_info['id'])        
        os.chdir(transformation_dir)

        transformation = model.Session.query(Transformation).filter_by(package_id=package_info['id']).first()

        #create context and call transformation entry point
        context = ExtractionContext(transformation, model.Session)

        try:
            transformation_instance = self.get_instance(transformation_dir, transformation.mainclass)
            transformation_instance.start_transformation(context)
        except:
            comment = traceback.format_exc()
            context.finish_error(comment)
            log.info(comment)
