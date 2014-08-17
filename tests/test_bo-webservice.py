# -*- coding: utf-8 -*-
"""Simple FunkLoad test

$Id$
"""
import unittest
import json
from random import random
from funkload.FunkLoadTestCase import FunkLoadTestCase
from funkload.utils import Data


class BoWebservice(FunkLoadTestCase):
    """This test use a configuration file BlitzortungService.conf."""

    def setUp(self):
        """Setting up test."""
        self.logd("setUp")
        self.server_url = self.conf_get('main', 'url')

    def call_service(self, method_name, *args):
        jsonrpc_data = {}
        jsonrpc_data['id'] = 0
        jsonrpc_data['method'] = method_name
        jsonrpc_data['params'] = args
        jsonrpc_data_string = json.dumps(jsonrpc_data)
        self.logd("jsonrpc_data: '%s'" % (jsonrpc_data_string))
        return self.post(self.server_url, params=Data('text/json', jsonrpc_data_string))

    def get_response(self):
        self.logd("jsonrpc_response: '%s'" % self.getBody())
        response = json.loads(self.getBody())

        if 'fault' in response:
            raise Exception(response['faultString'])

        return (response[0] if response and len(response) > 0 else None)

    def test_check(self):
        ret = self.call_service('check')
        self.logd('ret: %s' % str(ret).strip())
        response = self.get_response()
        self.assert_('count' in response, 'response contains element "count"')
        self.assert_(isinstance(response['count'], int), '"count" element has int type')

    def test_get_strikes(self):
        ret = self.call_service('get_strokes', 60)
        self.logd('ret: %s' % str(ret).strip())
        response = self.get_response()

    def test_get_strikes_raster(self):
        ret = self.call_service('get_strokes_raster', 60, 10000, 1)
        self.logd('ret: %s' % str(ret).strip())
        response = self.get_response()

    def tearDown(self):
        """Setting up test."""
        self.logd("tearDown.\n")


if __name__ in ('main', '__main__'):
    unittest.main()
