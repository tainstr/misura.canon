#!/usr/bin/python
# -*- coding: utf-8 -*-
import os
import unittest

import tempfile
import tables
import numpy as np

from misura.canon import indexer
from misura.canon.csutil import flatten as flat
from misura.canon import reference
#@unittest.skip('')


class ReferenceFunctions(unittest.TestCase):

    """Tests unbound functions in the reference module"""

    def test_binary_cast(self):
        f = 1.23456789
        b = reference.binary_cast([f], 'd', 'bbbbbbbb')
        self.assertEqual(len(b), 8)
        f1 = reference.binary_cast(b, 'bbbbbbbb', 'd')[0]
        self.assertEqual(f, f1)

#@unittest.skip('')


class Reference(unittest.TestCase):

    """Tests unbound functions in the reference module"""

    def setUp(self):
        self.ref = reference.Reference(
            False, '', {'name': 'Test', 'handle': 'test'})


def mkfile():
    path = tempfile.mktemp()
    f = tables.openFile(path, mode='w')
    f.close()
    return indexer.SharedFile(path)


class OutFile(unittest.TestCase):

    """HDF File management utility TestCase"""
    outfile = False
    refClass = False
    _outfile = False
    keep = False
    __test__ = False  # nosetests will ignore this

    def rand(self, *a, **k):
        """Reimplement in specific files to obtain random data"""
        assert False

    def mkfile(self):
        self.outfile = mkfile()
        self.path = self.outfile.get_path()
        destfile = mkfile()  # destination file for copy operations
        self.destpath = destfile.get_path()
        destfile.close()

    def tearDown(self):
        if self.outfile is False:
            return
        self.outfile.close()
        if not self.keep:
            os.remove(self.path)
            os.remove(self.destpath)

    def check_decode(self, encoded, decoded):
        t, val = decoded
        dec = self.refClass.decode(encoded)
        print 'check_decode', dec
        t1, val1 = dec
        self.assertEqual(val, val1)
        self.assertEqual(t, t1)

#	@unittest.skip('')
    def test_encode_decode(self):
        if self.refClass is False:
            raise unittest.SkipTest('')
        t, data = self.rand(1)
        out = self.refClass.encode(t, data)
        self.assertNotEqual(out, None)
        self.check_decode(out, (t, data))

#	@unittest.skip('')
    def test_commit(self):
        if self.refClass is False:
            raise unittest.SkipTest('')
        print 'test_commit', type(self)
        self.mkfile()
        opt = {'handle': 'test', 'name': 'Test',
               'unit': 'dummy', 'format': 'm4'}
        ref = self.refClass(self.outfile, '/', opt)
        # Check attributes
        opt['_reference_class'] = ref.__class__.__name__
        self.assertEqual(opt, ref.get_attributes())
        data = []
        for i in range(1, 10):
            data.append(self.rand(float(i)))
        self.assertTrue(ref.commit(data))
        rdata = ref[:]
        print 'data', flat(data)
        print 'ref', flat(ref[:])
        self.assertSequenceEqual(flat(data), flat(ref[:]))
        data2 = []
        for i in range(11, 20):
            data2.append(self.rand(float(i)))
        ref.commit(data2)
        data3 = ref[:]
        # Check that data is not committed more than one time
        self.assertEqual(len(data3), 18)
        self.assertEqual(len(ref), 18)
        return


class Array(OutFile):
    refClass = reference.Array
    __test__ = True

    @classmethod
    def rand(cls, t):
        return [t, t * 10]


class VariableLength(object):

    def test_compress(self):
        data = np.random.random(10, 10)
        dataz = reference.VariableLength.compress(data)
        data1 = reference.VariableLength.decompress(dataz)
        self.assertEqual(flat(data), flat(data1))


class Rect(OutFile):
    __test__ = True
    refClass = reference.Rect

    @classmethod
    def rand(cls, t):
        b = t * 10
        return [t, [b + 1, b + 2, b + 3, b + 4]]


class Meta(OutFile):
    refClass = reference.Meta

    @classmethod
    def rand(cls, t):
        b = t * 10
        return [t, {'value': b + 1, 'time': b + 2, 'temp': b + 3}]


@unittest.skip('')
class Image(OutFile):
    __test__ = True
    refClass = reference.Image

    @classmethod
    def rand(self, t=-1):
        """Produce random data"""
        if t < 0:
            t = np.random.random() * 1000
        img = np.random.random((480, 640)) * 2
        img = img.astype('i8')
        img *= 255
        return t, img

    def check_decode(self, encoded, decoded):
        t, img = decoded
        t1, img1 = self.refClass.decode(encoded)
        self.assertTrue((img.flatten() == img1.flatten()).all())
        self.assertEqual(t, t1)

    def test_encode(self):
        m = np.array([[0, 1, 0],
                      [0, 0, 1]])
        m *= 255
        h, w = m.shape
#		# Theoretical compressed array
#		mcp=[0,0,0,0,w,h,-1,1,-3,1]
#		cp=self.refClass.encode(0,m)
#		self.assertEqual(list(cp),mcp)


class Profile(OutFile):
    __test__ = True
    refClass = reference.Profile

    @classmethod
    def rand(cls, t=-1):
        """Produce random profile-like data"""
        if t < 0:
            t = np.random.random() * 1000
        w = 640
        h = 480
        x = np.random.random(10) * w
        y = np.random.random(10) * h
        return t, ((w, h), x.astype('i8'), y.astype('i8'))

    def check_decode(self, encoded, decoded):
        t, ((w, h), x, y) = decoded
        t1, ((w1, h1), x1, y1) = self.refClass.decode(encoded)
        self.assertEqual(t, t1)
        self.assertEqual(w, w1)
        self.assertEqual(h, h1)
        self.assertEqual(list(x.flatten()), list(x1.flatten()))
        self.assertEqual(list(y.flatten()), list(y1.flatten()))


class CumulativeProfile(Profile):
    __test__ = False
    refClass = reference.CumulativeProfile

    @classmethod
    def rand(cls, t=-1):
        if t < 0:
            t = np.random.random() * 1000
        w = 640
        h = 480

        def coord(n):
            c = np.random.random(n) - 0.5
            c = np.sign(c) * (np.abs(c) > 0.16)
            return c
        x = coord(10)
        y = coord(10)
        # If both are 0, increase x
        x += (x == 0) * (y == 0)
        x = 300 + np.cumsum(x)
        y = 300 + np.cumsum(y)
        return t, ((w, h), x.astype('uint16'), y.astype('uint16'))

#@unittest.skip('')


class Binary(OutFile):
    __test__ = True
    refClass = reference.Binary

    @classmethod
    def rand(self, t=-1):
        """Produce random binary blob"""
        if t < 0:
            t = np.random.random() * 1000
        idat = np.random.random(1000) * 255
        idat = idat.astype(np.uint8)
        data = ''.join(map(chr, idat))
        return t, data

    def check_decode(self, encoded, decoded):
        t, dat = decoded
        t1, dat1 = self.refClass.decode(encoded)
        self.assertEqual(t, t1)
        self.assertEqual(dat, dat1)


class Log(OutFile):
    __test__ = True
    refClass = reference.Log

    @classmethod
    def rand(cls, t):
        return [1. * t, [10, 'message%i' % t]]


if __name__ == "__main__":
    unittest.main()
