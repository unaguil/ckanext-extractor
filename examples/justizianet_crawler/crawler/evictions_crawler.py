# encoding: utf-8

import urllib
import urllib2
import re
import simplejson
import httplib
from ConfigParser import ConfigParser

from BeautifulSoup import BeautifulSoup
from datetime import datetime

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

from model import *
from extra_data import *

import os

opener = urllib2.build_opener(urllib2.HTTPCookieProcessor)

#keys 
URL = u'url'
LOCALIDAD = u'localidad'
PARTIDO_JUDICIAL = u'partido judicial'
RESUMEN = u'resumen'
CANCELADO = u'cancelado'
ORGANO_JUDICIAL = u'órgano judicial'
PROCEDIMIENTO_JUDICIAL = u'procedimiento judicial'
TELEFONO = u'teléfono'
VALORACION = u'valoración'
DEPOSITO = u'depósito'
NIG = u'nig'
DIRECCION = u'dirección'
HORA = u'hora'
DIA = u'día'

#
cod_eviction = 'hipotecari'

class Crawler():

    def isAnImportantWord(self, phrase, wordlist):
        for w in wordlist:
            if w in phrase.lower():
                return True
        return False

    def connection(self, url):
        print 'Connecting to URL %s' % url
        soup = None
        try:
            response = opener.open(url)
            data = response.read()
            soup = BeautifulSoup(data, convertEntities=BeautifulSoup.HTML_ENTITIES)
            soup.prettify()
            return soup
        except:
            print "ERROR accessing URL %s" % url
            return None

    def processPages(self, evlisthtml, cpartido):
        #process received rows
        for ev in evlisthtml.findAll('tr'):
            if (ev.has_key('class') and "vevent" in ev['class']):
                cancelled = True if ev.has_key('title') else False
                isfinalpage = False

                #obtain tags
                for evspan in ev.findAll('span'):
                    if (evspan.has_key('class') and "summary" in evspan['class']):
                        detailsurl = evspan.find('a')['href']
                        title = evspan.find('a').contents[0]

                #check if entry must be stored
                if self.isAnImportantWord(title, importantworddict):
                    self.scrappEviction(cpartido=cpartido, url=detailsurl, title=title, cancelled=cancelled)

    def context_generator(self, extraction_context):
        context = extraction_context.get_current_context()
        if len(context) == 0:
            #initialize status and store it
            for cpartido in cpartidos.keys():
                context[cpartido] = None
            extraction_context.update_context(context)
        
        for cpartido in context.keys():
            if context[cpartido] is None:
                start_date = datetime(2005, 01, 01)
                end_date = datetime.now()
            else:
                start_date = context[cpartido][1]
                end_date = datetime.now()

            context[cpartido] = (start_date, end_date)
            yield cpartido, start_date, end_date
            extraction_context.update_context(context)

    def scrappList(self, extraction_context):
        for cpartido, start_date, end_date in self.context_generator(extraction_context):
            print 'Downloading %s from %s to %s' % (cpartido, start_date, end_date)
            page = 1
            isfinalpage = False
            while not isfinalpage:
                isfinalpage = True
                params = {
                    'cfechaD': start_date.strftime('%d/%m/%Y'),
                    'cfechaH': end_date.strftime('%d/%m/%Y'),
                    'cpartido': cpartido,
                    'ctipo': 'INMU',
                    'primerElem': page
                }

                evlisthtml = self.connection(self.data_url + '?' + urllib.urlencode(params))
                self.processPages(evlisthtml, cpartido)
            
                page += self.pagination

            print 'Finished download of %s from %s to %s' % (cpartido, start_date, end_date)

        extraction_context.finish_ok('Extraction correctly finished')

    def scrappEviction(self, cpartido, url, title, cancelled):
        evicdict = {}

        evhtml = self.connection(url)
        if evhtml is not None:
            for det in evhtml.findAll('div'):
                if (det.has_key('class') and "fila" in det['class']):
                    for datum in det.findAll('div'):
                        if (datum.has_key('class') and "etiqueta" in datum['class']):
                            etiqueta = u''
                            try:
                                etiqueta = datum.contents[0].contents[0]
                            except:
                                etiqueta = datum.contents[0]
                            if etiqueta[-1] == ':':
                                etiqueta = etiqueta[:-1]
                        elif (datum.has_key('class') and "dato" in datum['class']):
                            dato = u''
                            try:
                                dato = datum.contents[0].contents[0]
                            except:
                                dato = datum.contents[0]

                    evicdict[etiqueta.lower()] = dato

            evicdict[URL] = url
            evicdict[PARTIDO_JUDICIAL] = cpartidos[cpartido]
            evicdict[RESUMEN] = title
            evicdict[CANCELADO] = cancelled

            #check valid 'Localidad'
            if not LOCALIDAD in evicdict or evicdict[LOCALIDAD] == '.':
                for municipio in municipios:
                    if municipio.lower() in title.lower():
                        evicdict[LOCALIDAD] = municipio
                        break

            if not LOCALIDAD in evicdict or not evicdict[LOCALIDAD] in municipios:
                evicdict[LOCALIDAD] = evicdict[PARTIDO_JUDICIAL]

            if not DIRECCION in evicdict:
                evicdict[DIRECCION] = ''

            if cod_eviction in evicdict[PROCEDIMIENTO_JUDICIAL]:
                print "Storing to DB URL %s" % url
                day, month, year = evicdict[DIA].split('/')
                hour, minute = evicdict[HORA].split(':')
     
                evicdate = datetime(int(year), int(month), int(day), int(hour), int(minute))

                cpartidolocation = self.session.query(Municipio).filter_by(nombre = evicdict[PARTIDO_JUDICIAL]).first()
                partidojudicial = self.session.query(PartidoJudicial).filter_by(municipio = cpartidolocation).first()
                if partidojudicial is None:
                    partidojudicial = PartidoJudicial(evicdict[PARTIDO_JUDICIAL], evicdict[ORGANO_JUDICIAL], evicdict[TELEFONO], cpartidolocation)
                    self.session.add(partidojudicial)
                    self.session.commit()

                evicval = evicdict[VALORACION][:-2].replace('.', '').replace(',', '.')
                evicdep = evicdict[DEPOSITO][:-2].replace('.', '').replace(',', '.')
                eviction = Desahucio(evicdate, evicdict[URL], float(evicval), evicdict[CANCELADO], float(evicdep),
                    evicdict[RESUMEN], evicdict[PROCEDIMIENTO_JUDICIAL], evicdict[DIRECCION], evicdict[NIG],
                    self.session.query(Municipio).filter_by(nombre = evicdict[LOCALIDAD]).first(), partidojudicial)

                self.session.add(eviction)
                self.session.commit()

    def start_transformation(self, extraction_context):
        configParser = ConfigParser()
        configParser.read('crawler.cfg')
        db_connection = configParser.get('database', 'db_connection')

        self.pagination = int(configParser.get('crawler', 'pagination'))
        self.data_url = configParser.get('crawler', 'data_url')

        engine = create_engine(db_connection, convert_unicode=True, pool_recycle=3600)
        self.session = sessionmaker(bind = engine)()

        self.scrappList(extraction_context)

        self.session.commit()
        self.session.close()

    def create_db(self):
        configParser = ConfigParser()
        configParser.read('crawler.cfg')
        db_connection = configParser.get('database', 'db_connection')
        engine = create_engine(db_connection, convert_unicode=True, pool_recycle=3600)

        #destroying database schema
        print 'Dropping tables'
        Base.metadata.drop_all(bind = engine)

        #creating database schema
        print 'Creating tables'
        Base.metadata.create_all(bind = engine)

        #add municipality information
        session = sessionmaker(bind = engine)()

        for municipio in municipios:
            munidb = Municipio(municipio)
            session.add(munidb)

        session.commit()
        session.close()

