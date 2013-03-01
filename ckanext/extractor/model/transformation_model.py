from sqlalchemy import Column, String, LargeBinary, DateTime, Integer, Boolean
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Transformation(Base):
    __tablename__ = 'transformations'

    package_name = Column(String, primary_key=True)
    filename = Column(String, nullable=False)
    data = Column(LargeBinary, nullable=False)
    timestamp = Column(DateTime, nullable=False)
    mainclass = Column(String, nullable=False)
    enabled = Column(Boolean, nullable=False)

    def __init__(self, package_name, filename=None, data=None, timestamp=None, mainclass=None, enabled=False):
        self.package_name = package_name
        self.filename = filename
        self.data = data
        self.timestamp = timestamp
        self.mainclass = mainclass
        self.enabled = enabled

    def __repr__(self):
        return '<Transformation package_name: %s filename %s timestamp: %s mainclass: %s enabled: %s>' % (self.package_name, self.filename, self.timestamp, self.mainclass, self.enabled)
