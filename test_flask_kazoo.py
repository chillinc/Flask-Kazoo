import unittest
import uuid

from flask import Flask
from flask.ext.kazoo import Kazoo


class KazooTestCase(unittest.TestCase):

    def setUp(self):
        app = Flask(__name__)
        app.config['TESTING'] = True
        app.config['ZOOKEEPER_HOSTS'] = 'localhost:2181'
        app.config['ZOOKEEPER_TIMEOUT'] = 3
        self.kazoo = Kazoo(app)
        self.app = app

        @app.route('/write/<path>')
        def write(path):
            return str(self.kazoo.client.create('/kazoo-test/%s' % path, b'OK', makepath=True))

        @app.route('/read/<path>')
        def read(path):
            return self.kazoo.client.get('/kazoo-test/%s' % path)[0]

        with self.app.app_context():
            self.kazoo.client.delete('/kazoo-test/test-kazoo-node')

    def teardown(self):
        self.kazoo.client.delete('/kazoo-test/test-kazoo-node')

    def test_kazoo_set(self):
        with self.app.test_client() as c:
            results = c.get('/write/test-kazoo-node')
            self.assertEqual(results.status_code, 200)

    def test_kazoo_read(self):
        path = 'test-kazoo-node'
        value = str(uuid.uuid1())
        with self.app.app_context():
            self.kazoo.client.create('/kazoo-test/%s' % path, value, makepath=True)

        with self.app.test_client() as c:
            results = c.get('/read/%s' % path)
            self.assertEqual(results.data, value)


if __name__ == '__main__':
    unittest.main()
