class BaseConfig(object):
    '''
    Base config class
    '''
    DEBUG = True
    TESTING = False


class ProductionConfig(BaseConfig):
    '''
    Production specific config
    '''
    DEBUG = False

class DevelopmentConfig(BaseConfig):
    '''
    Development environment specific configuration
    '''
    DEBUG = True
    TESTING = True
    THREADED = True
    kafka = {'hosts': ['localhost:9092'],
             'raw_topic': 'sensor',
             'avg_topic': 'averages'
            }
