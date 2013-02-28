# -*- coding: utf8 -*- 

from ckan.lib.base import render, c
from logging import getLogger
from ckan.controllers.package import PackageController
from pylons import request

import os.path
import shutil
from zipfile import ZipFile

log = getLogger(__name__)

class ExtractorController(PackageController):
        
    def show_extractor_config(self, id):
        log.info('Showing extractor configuration for id: %s' % id)         

        # using default functionality
        template = self.read(id)

        c.error = False

        #rendering using default template
        return render('extractor/read.html')

    def extract_transformation(self, id):
    	log.info('Processing submitted transformation for id: %s' % id)

    	 # using default functionality
        template = self.read(id)

        filename = request.params['transformation_code'].filename
        submitted_file = request.params['transformation_code'].file

        try:
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
	        log.info('Extracting file %s to directory %s' % (filename, package_dir))
	        zipfile.extractall()
        except Exception:
        	c.error = True
        	c.error_message = 'Problem handling uploaded file %s' % filename

        #rendering using default template
    	return render('extractor/read.html')


