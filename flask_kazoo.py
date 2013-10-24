
from flask import _app_ctx_stack
from flask.signals import Namespace

from kazoo.client import KazooClient

__all__ = ('Kazoo', )

__version__ = '0.1'

_signals = Namespace()

connection_state_changed = _signals.signal('state-change')


def _get_client():
    _app_ctx_stack


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
        # Put cqlengine to application extensions
        if not 'kazoo' in app.extensions:
            app.extensions['kazoo'] = {}

        # Initialize connection and store it to extensions
        self.hosts = app.config['ZOOKEEPER_HOSTS']
        self.timeout = app.config['ZOOKEEPER_TIMEOUT']

        # Use the newstyle teardown_appcontext if it's available,
        if hasattr(app, 'teardown_appcontext'):
            app.teardown_appcontext(self.teardown)

    @property
    def client(self):
        ctx = _app_ctx_stack.top
        if ctx is not None:
            if not hasattr(ctx, 'kazoo_client'):
                ctx.kazoo_client = KazooClient(hosts=self.hosts)
                ctx.kazoo_client.start(self.timeout)
                ctx.kazoo_client.add_listener(self.connection_state_listener)
            return ctx.kazoo_client

    def teardown(self, exception):
        ctx = _app_ctx_stack.top
        if hasattr(ctx, 'kazoo_client'):
            ctx.kazoo_client.stop()

    def connection_state_listener(self, state):
        """Publishes state changes to a Flask signal"""
        connection_state_changed.send(self, state=state)
