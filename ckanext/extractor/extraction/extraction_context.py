# -*- coding: utf8 -*- 

from sqlalchemy import desc
from ckanext.extractor.model.transformation_model import Extraction
from datetime import datetime

WORKING = u'working'
ERROR = u'error'
OK = u'ok'

class ExtractionContext():

	def __init__(self, transformation, session):
		self.transformation = transformation
		self.session = session

		#initialize current status using database information if it exists
		self.extraction = session.query(Extraction).filter_by(transformation_id=transformation.package_id).order_by(desc(Extraction.start_date)).first()

		if self.extraction is None:
			self.extraction = Extraction(datetime.now(), '', WORKING)
			self.transformation.extractions.append(self.extraction)
		else:
			self.extraction = Extraction(datetime.now(), self.extraction.context, WORKING)
			self.transformation.extractions.append(self.extraction)

		self.session.merge(self.transformation)
		self.session.commit()

	def update_context(self, new_context):
		self.extraction.context = new_context
		session.merge(self.transformation)
		session.commit()

	def get_current_context(self):
		return self.extraction.context

	def finish_ok(self, comment):
		self.extraction.end_date = datetime.now()
		self.extraction.transformation_status = OK
		self.extraction.comment = comment
		self.session.merge(self.transformation)
		self.session.commit()

	def finish_error(self, comment):
		self.extraction.end_date = datetime.now()
		self.extraction.transformation_status = ERROR
		self.extraction.comment = comment
		self.session.merge(self.transformation)
		self.session.commit()