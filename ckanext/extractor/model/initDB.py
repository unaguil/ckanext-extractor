from sqlalchemy import create_engine

from transformation_model import Transformation

print 'Creating table for transformation storage'
engine = create_engine('postgresql://ckanuser:pass@localhost/ckantest', echo=True)

Transformation.metadata.drop_all(engine)
Transformation.metadata.create_all(engine)
