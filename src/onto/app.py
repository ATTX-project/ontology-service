import falcon
from onto.utils.logs import app_logger
from onto.api.healthcheck import HealthCheck
from onto.api.indexing import AliasesList, IndexClass

api_version = "0.2"  # TO DO: Figure out a better way to do versioning


def init_api():
    """Create the API endpoint."""
    ontoservice = falcon.API()

    ontoservice.add_route('/health', HealthCheck())

    ontoservice.add_route('/%s/alias/list' % (api_version), AliasesList())

    ontoservice.add_route('/%s/data/index' % (api_version), IndexClass())

    app_logger.info('IndexService REST API is running.')
    return ontoservice


# if __name__ == '__main__':
#     init_api()
