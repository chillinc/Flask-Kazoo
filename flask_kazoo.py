
from werkzeug.local import LocalProxy

from flask import current_app
from flask.signals import Namespace

from kazoo.client import KazooClient

__all__ = ('Kazoo', )

__version__ = '0.1'

_signals = Namespace()

connection_state_changed = _signals.signal('state-change')


def _get_client():
    kazoo_client = current_app._get_current_object().extensions['kazoo']['client']
    return kazoo_client

kazoo_client = LocalProxy(_get_client)


class Kazoo(object):
    """Kazoo Client support for Flask."""

    def __init__(self, app=None):
        """
        If app argument provided then initialize cqlengine connection using
        application config values.

        If no app argument provided you should do initialization later with
        :meth:`init_app` method.

        :param app: Flask application instance.

        """
        self.app = app
        if app is not None:
            self.init_app(app)

    def init_app(self, app):
        """
        Read kazoo settings from app configuration,
        setup kazoo client for application

        :param app: Flask application instance.

        """
        app.config.setdefault('KAZOO_HOSTS', '127.0.0.1:2181')
        app.config.setdefault('KAZOO_START_TIMEOUT', 3)
        app.config.setdefault('KAZOO_START_BLOCKING', False)

        app.config.setdefault('KAZOO_SESSION_TIMEOUT', 10.0)  # kazoo default

        app.config.setdefault('KAZOO_DEFAULT_RETRY', True)
        app.config.setdefault('KAZOO_RETRY_MAX_DELAY_SECONDS', 60 * 60)  # kazoo default of 1hr.

        # Put cqlengine to application extensions
        if not 'kazoo' in app.extensions:
            app.extensions['kazoo'] = {}

        # Initialize connection and store it to extensions
        if app.config['KAZOO_DEFAULT_RETRY']:
            retry_kwargs = {
                'max_delay': app.config['KAZOO_RETRY_MAX_DELAY_SECONDS']
            }
        else:
            retry_kwargs = None

        kazoo_client = KazooClient(hosts=app.config['KAZOO_HOSTS'],
                                   timeout=app.config['KAZOO_SESSION_TIMEOUT'],
                                   connection_retry=retry_kwargs,
                                   command_retry=retry_kwargs)

        if app.config['KAZOO_START_BLOCKING']:
            kazoo_client.start(app.config['KAZOO_START_TIMEOUT'])
        else:
            kazoo_client.start_async()

        kazoo_client.add_listener(self.connection_state_listener)

        app.extensions['kazoo']['client'] = kazoo_client

    def connection_state_listener(self, state):
        """Publishes state changes to a Flask signal"""
        connection_state_changed.send(self, state=state)
