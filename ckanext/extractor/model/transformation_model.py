from sqlalchemy import Column, String, LargeBinary, DateTime, Integer
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Transformation(Base):
    __tablename__ = 'transformations'

    package_name = Column(String, primary_key=True)
    code = Column(LargeBinary, nullable=False)
    timestamp = Column(DateTime, nullable=False)
    mainclass = Column(String, nullable=False)

    def __init__(self, package_name, code, timestamp, mainclass):
        self.package_name = package_name
        self.code = code
        self.timestamp = timestamp
        self.mainclass = mainclass

    def __repr__(self):
        return '<Transformation package_name: %s timestamp: %s mainclass: %s>' % (self.package_name, self.timestamp, self.mainclass)
