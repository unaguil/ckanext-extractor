# -*- coding: utf8 -*- 

from ckan.lib.base import render
from logging import getLogger
from ckan.controllers.package import PackageController
from pylons import request

log = getLogger(__name__)

class ExtractorController(PackageController):
        
    def show_extractor_config(self, id):
        log.info('Showing extractor configuration for id: %s' % id)         

        # using default functionality
        template = self.read(id)

        #rendering using default template
        return render('extractor/read.html')

    def extract_transformation(self, id):
    	log.info('Processing submitted transformation for id: %s' % id)

    	 # using default functionality
        template = self.read(id)

        log.info('Filename: %s ' request.params['transformation_code'].filename)
        file = request.params['transformation_code'].file.read()

        #rendering using default template
    	return render('extractor/read.html')


