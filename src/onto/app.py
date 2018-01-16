import falcon
from onto.utils.logs import app_logger
from onto.api.healthcheck import HealthCheck

api_version = "0.2"  # TO DO: Figure out a better way to do versioning


def init_api():
    """Create the API endpoint."""
    ontoservice = falcon.API()

    ontoservice.add_route('/health', HealthCheck())

    app_logger.info('IndexService REST API is running.')
    return ontoservice


# if __name__ == '__main__':
#     init_api()
