# -*- coding: utf8 -*- 

from sqlalchemy import desc
from ckanext.extractor.model.transformation_model import Extraction
from datetime import datetime

class ExtractionContext():

	def __init__(self, transformation, session):
		self.transformation = transformation
		self.session = session

		#initialize current status using database information if it exists
		self.extraction = session.query(Extraction).filter_by(transformation_id=transformation.package_id).order_by(desc(Extraction.start_date)).first()

		if self.extraction is None or self.extraction.transformation_status in ['ok', 'error']:
			self.extraction = Extraction(transformation.package_id, datetime.now(), None, 'working')
		elif self.extraction.transformation_status in ['ok', 'error']:
			self.extraction = Extraction(transformation.package_id, datetime.now(), self.extraction.context, 'working')

		self._initialize(self.extraction.context)

	def _initialize(self, context):
		#call the plugin with the current context
		pass

	def update_context(new_context):
		self.extraction.context = new_context
		session.merge(self.extraction)
		session.commit()

	def get_current_context():
		return self.extraction.context

	def finish_ok(comment):
		self.extraction.end_date = datetime.now()
		self.extraction.transformation_status = 'ok'
		self.extraction.comment = comment
		session.merge(self.extraction)
		session.commit()

	def finish_error(comment):
		self.extraction.end_date = datetime.now()
		self.extraction.transformation_status = 'error'
		self.extraction.comment = comment
		session.merge(self.extraction)
		session.commit()