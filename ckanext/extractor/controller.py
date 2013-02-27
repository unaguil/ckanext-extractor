# -*- coding: utf8 -*- 

from ckan.lib.base import render
from logging import getLogger
from ckan.controllers.package import PackageController

log = getLogger(__name__)

class ExtractorController(PackageController):
        
    def show_extractor_config(self, id):
        log.info('Showing extractor configuration for id: %s' % id)         

        # using default functionality
        template = self.read(id)

        #rendering using default template
        return render('extractor/read.html')
