from sqlalchemy import create_engine

from transformation_model import Transformation

from transformation_model import RunningTask

USER = 'ckanuser'
PASS = 'pass'

print 'Creating table for transformation storage'
engine = create_engine('postgresql://%s:%s@localhost/ckantest' % (USER, PASS), echo=True)

Transformation.metadata.drop_all(engine)
Transformation.metadata.create_all(engine)

RunningTask.metadata.drop_all(engine)
RunningTask.metadata.create_all(engine)


