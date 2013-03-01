from sqlalchemy import Column, String, LargeBinary, DateTime, Integer, Boolean
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Transformation(Base):
    __tablename__ = 'transformations'

    package_name = Column(String, primary_key=True)
    filename = Column(String, nullable=False)
    code = Column(LargeBinary, nullable=False)
    timestamp = Column(DateTime, nullable=False)
    mainclass = Column(String, nullable=False)
    enabled = Column(Boolean, nullable=False)

    def __init__(self, package_name, filename, code, timestamp, mainclass, enabled=True):
        self.package_name = package_name
        self.filename = filename
        self.code = code
        self.timestamp = timestamp
        self.mainclass = mainclass
        self.enabled = enabled

    def __repr__(self):
        return '<Transformation package_name: %s filename %s timestamp: %s mainclass: %s enabled: %s>' % (self.package_name, self.filename, self.timestamp, self.mainclass, self.enabled)
