#!/usr/bin/python
# -*- coding: utf-8 -*-
import unittest

from misura.canon.option import common_proxy

class FakeProxy():
    def toPath(self, passed_argument):
        return passed_argument

class CommonProxy(unittest.TestCase):
    def test_from_column_basic(self):
        toPath_passed_argument, name = common_proxy.from_column("another/path/to/a/leaf", FakeProxy())

        self.assertEqual("leaf", name)
        self.assertEqual(["another", "path", "to", "a"], toPath_passed_argument)

    def test_from_column_removes_prefix_from_path(self):
        toPath_passed_argument, name = common_proxy.from_column("123:any/path/to/a/leaf2", FakeProxy())

        self.assertEqual("leaf2", name)
        self.assertEqual(["any", "path", "to", "a"], toPath_passed_argument)

    def test_from_column_removes_summary_from_path(self):
        toPath_passed_argument, name = common_proxy.from_column("/summary/path/to/a/leaf", FakeProxy())

        self.assertEqual(["path", "to", "a"], toPath_passed_argument)


if __name__ == "__main__":
    unittest.main()
