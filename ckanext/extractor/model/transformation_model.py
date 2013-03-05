# -*- coding: utf8 -*- 

from sqlalchemy import Column, String, LargeBinary, DateTime, Integer, Boolean
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Transformation(Base):
    __tablename__ = 'transformations'

    package_id = Column(String, primary_key=True)
    filename = Column(String, nullable=False)
    data = Column(LargeBinary, nullable=False)
    timestamp = Column(DateTime, nullable=False)
    mainclass = Column(String, nullable=False)
    enabled = Column(Boolean, nullable=False)

    def __init__(self, package_id, filename=None, data=None, timestamp=None, mainclass=None, enabled=True):
        self.package_id = package_id
        self.filename = filename
        self.data = data
        self.timestamp = timestamp
        self.mainclass = mainclass
        self.enabled = enabled

    def __repr__(self):
        return '<Transformation package_id: %s filename: %s timestamp: %s mainclass: %s enabled: %s>' % (self.package_id, self.filename, self.timestamp, self.mainclass, self.enabled)

class Extraction(Base):
    __tablename__ = 'extractions'

    id = Column(Integer, primary_key=True)
    start_date = Column(DateTime, primary_key=True)
    transformation_id = Column(String, nullable=False)
    end_date = Column(DateTime)
    context = Column(String, nullable=False)
    transformation_status = Column(String, nullable=False)
    comment = Column(String)

    def __init__(self, transformation_id, start_date, context, transformation_status):
        self.transformation_id = transformation_id
        self.start_date = start_date
        self.context = context
        self.transformation_status = transformation_status

    def __repr__(self):
        return '<Extraction transformation_id: %s start_date: %s end_date: %s context: %s transformation_status: %s>' % (self.transformation_id, self.start_date, self.end_date, self.context, self.transformation_status)
