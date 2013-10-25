import unittest
import uuid

from kazoo.exceptions import NoNodeError

from flask import Flask
from flask.ext.kazoo import Kazoo, kazoo_client


class KazooTestCase(unittest.TestCase):

    def setUp(self):
        self.app = Flask(__name__)
        self.app.config['TESTING'] = True
        self.kazoo = Kazoo(self.app)

        self.node_prefix = '/kazoo-test/'
        self.node_name = 'test-kazoo-node'
        self.node_value = uuid.uuid1().bytes
        self.app.extensions['kazoo']['client'].create('/kazoo-test/%s' % self.node_name, self.node_value, makepath=True)

        @self.app.route('/write/<path>')
        def write(path):
            return str(kazoo_client.set(self.node_prefix + path, b'OK'))

        @self.app.route('/read/<path>')
        def read(path):
            return kazoo_client.get(self.node_prefix + path)[0]

    def tearDown(self):
        try:
            self.app.extensions['kazoo']['client'].delete(self.node_prefix + self.node_name)
        except NoNodeError:
            print "Teardown got NoNodeError"

    def test_kazoo_set(self):
        with self.app.test_client() as c:
            results = c.get('/write/%s' % self.node_name)
            self.assertEqual(results.status_code, 200)

    def test_kazoo_read(self):
        with self.app.test_client() as c:
            results = c.get('/read/%s' % self.node_name)
            self.assertEqual(results.data, self.node_value)


if __name__ == '__main__':
    unittest.main()
