#!/usr/bin/python
# -*- coding: utf-8 -*-
import unittest
from misura import parameters as params
from misura.canon import option
from misura.canon import indexer
from misura.canon import csutil

path = params.testdir + 'storage/hsm_test' + params.ext
# path='/opt/shared_data/hsm/test_polvere_18.h5'


class ConfigurationProxy(unittest.TestCase):

    """Tests the option.Conf object"""
    @csutil.profile
    def test(self):
        sh = indexer.SharedFile(path)
        sh.load_conf()
        sh.run_scripts(sh.conf.hsm)
        sh.close()
        sh.conf.kiln.add_option('testOpt','Boolean',True,'Test option')
        self.assertTrue(sh.conf.kiln['testOpt'])


if __name__ == "__main__":
    unittest.main(verbosity=2)
