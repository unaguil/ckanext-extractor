# -*- coding: utf8 -*- 

from sqlalchemy import Column, String, LargeBinary, DateTime, Integer, Boolean, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, backref

Base = declarative_base()

class Transformation(Base):
    __tablename__ = 'transformations'

    package_id = Column(String, primary_key=True)
    filename = Column(String, nullable=False)
    data = Column(LargeBinary, nullable=False)
    timestamp = Column(DateTime, nullable=False)
    enabled = Column(Boolean, nullable=False)
    minute = Column(String, nullable=False)
    hour = Column(String, nullable=False)
    day_of_week = Column(String, nullable=False)
    output_dir = Column(String, nullable=False)
    extractions = relationship("Extraction", backref="transformation")

    def __init__(self, package_id, filename=None, data=None, timestamp=None,
        enabled=True, minute='59', hour='23', day_of_week='*', output_dir=None):
        self.package_id = package_id
        self.filename = filename
        self.data = data
        self.timestamp = timestamp
        self.enabled = enabled
        self.minute = minute
        self.hour = hour
        self.day_of_week = day_of_week
        self.output_dir = output_dir

    def __repr__(self):
        return '<Transformation package_id: %s filename: %s timestamp: %s enabled: %s crontab: (%s %s %s) output_dir %s>' % (self.package_id,
            self.filename, self.timestamp, self.mainclass, self.enabled,
            self.minute, self.hour, self.day_of_week, self.output_dir)

class Extraction(Base):
    __tablename__ = 'extractions'

    id = Column(Integer, primary_key=True)
    start_date = Column(DateTime, primary_key=True)
    transformation_id = Column(String, ForeignKey('transformations.package_id'))
    end_date = Column(DateTime)
    context = Column(String, nullable=False)
    transformation_status = Column(String, nullable=False)
    comment = Column(String)

    def __init__(self, start_date, context, transformation_status):
        self.start_date = start_date
        self.context = context
        self.transformation_status = transformation_status

    def __repr__(self):
        return '<Extraction transformation_id: %s start_date: %s end_date: %s context: %s transformation_status: %s>' % (self.transformation.package_id, self.start_date, self.end_date, self.context, self.transformation_status)

class RunningTask(Base):
    __tablename__ = 'RunningTasks'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    task_name = Column(String)
    start_time = Column(DateTime)
    
    def __init__(self, task_name, start_time):
        self.task_name = task_name
        self.start_time = start_time
        
    def __repr__(self):
        return '<RunningTask id: %s task_name: %s start_time: %s>' % (self.id, self.task_name, self.start_time)
