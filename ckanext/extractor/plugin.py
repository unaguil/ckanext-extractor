# -*- coding: utf8 -*- 

from ckan.plugins import SingletonPlugin, IGenshiStreamFilter, implements, IConfigurer, IRoutes
from logging import getLogger
from pylons import request
from genshi.input import HTML
from genshi.filters.transform import Transformer
import os

log = getLogger(__name__)

class ExtractorExtension(SingletonPlugin):
    
    implements(IConfigurer, inherit=True)
    implements(IGenshiStreamFilter, inherit=True)
    implements(IRoutes, inherit=True)
    
    def update_config(self, config):
        here = os.path.dirname(__file__)
        rootdir = os.path.dirname(os.path.dirname(here))
        our_public_dir = os.path.join(rootdir, 'ckanext', 
				'extractor', 'theme', 'public')
                                      
        template_dir = os.path.join(rootdir, 'ckanext',
				'extractor', 'theme', 'templates')
				
        # set resource and template overrides
        config['extra_public_paths'] = ','.join([our_public_dir,
                config.get('extra_public_paths', '')])
                
        config['extra_template_paths'] = ','.join([template_dir,
				config.get('extra_template_paths', '')])
    
    def filter(self, stream):
        routes = request.environ.get('pylons.routes_dict')
        log.info(routes)
        if routes.get('controller') in ('package', 'related',
			'ckanext.extractor.controller:ExtractorController') :
               stream = stream | Transformer('//ul[@class="nav nav-pills"]').append(HTML(
                    
                    '''<li class>
                        <a class href="/dataset/extractor/%s">
                            <img src="/icons/rdf_flyer.24" height="16px" width="16px" alt="None" class="inline-icon ">
                            Source Extractor
                        </a>
                    </li>''' % routes.get('id')
                    
                ))

        return stream
       
    def before_map(self, map):
        map.connect('/dataset/extractor/{id}',
            controller='ckanext.extractor.controller:ExtractorController',
            action='show_extractor_config')

        map.connect('/dataset/extractor/{id}/transformation_submit',
            controller='ckanext.extractor.controller:ExtractorController',
            action='submit_transformation')

        map.connect('/dataset/extractor/{id}/transformation_download',
            controller='ckanext.extractor.controller:ExtractorController',
            action='download_transformation')

        map.connect('/dataset/extractor/{id}/transformation_launch',
            controller='ckanext.extractor.controller:ExtractorController',
            action='launch_transformation')

        map.connect('/dataset/extractor/{id}/show_message/{extraction_id}',
            controller='ckanext.extractor.controller:ExtractorController',
            action='show_message')

        map.connect('/dataset/extractor/{id}/perform_deploy',
            controller='ckanext.extractor.controller:ExtractorController',
            action='perform_deploy')

        return map
        