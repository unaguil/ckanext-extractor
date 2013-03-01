# -*- coding: utf8 -*- 

from ckan.lib.base import render, c, model, response
from ckan.logic import get_action
from logging import getLogger
from ckan.controllers.package import PackageController
from pylons import request

import os.path
import shutil
from zipfile import ZipFile
from datetime import datetime

from model.transformation_model import Transformation

log = getLogger(__name__)

class ExtractorController(PackageController):

    def get_transformation_data(self, id):
        transformation = model.Session.query(Transformation).filter_by(package_id=id).first()

        if transformation is not None:
            c.timestamp = transformation.timestamp.isoformat()
            c.mainclass = transformation.mainclass
            c.filename = transformation.filename
            c.enabled = transformation.enabled
            c.data = True
        else:
            c.data = False

        return c
        
    def show_extractor_config(self, id):
        log.info('Showing extractor configuration for id: %s' % id)         

        # using default functionality
        template = self.read(id)

        context = {'model': model, 'session': model.Session, 'user': c.user or c.author}
        package_info = get_action('package_show')(context, {'id': c.pkg.id})

        self.get_transformation_data(package_info['id'])

        c.error = False

        #rendering using default template
        return render('extractor/read.html')

    def extract_transformation(self, id):
        log.info('Processing submitted transformation for id: %s' % id)

         # using default functionality
        template = self.read(id)

        context = {'model': model, 'session': model.Session, 'user': c.user or c.author}
        package_info = get_action('package_show')(context, {'id': c.pkg.id})

        transformation = model.Session.query(Transformation).filter_by(package_id=package_info['id']).first()
        if transformation is None:
            transformation = Transformation(package_info['id'])

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

                #create transformations directory if it does not exist
                if not os.path.isdir('transformations'):
                    log.info('Creating transformations directory')
                    os.mkdir('transformations')

                #delete package directory if it exists
                package_dir = os.path.join('transformations', id)
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
            except Exception as e:
                c.error = True
                c.error_message = 'Problem extracting uploaded file %s (%s)' % (transformation.filename, e)
                return render('extractor/read.html')

        model.Session.merge(transformation)
        model.Session.commit()
        log.info("Transformation object stored for package '%s'" % id)

        #rendering using default template
        return render('package/read.html')

    def download_transformation(self, id):
        log.info('Dowloading transformation for id: %s' % id)

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
