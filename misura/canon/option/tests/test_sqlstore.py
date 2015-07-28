#!/usr/bin/python
# -*- coding: utf-8 -*-
import unittest
import os
from misura.canon import option
from misura.canon.option import get_typed_cols, get_insert_cmd, base_col_def, print_tree
import sqlite3
from misura import parameters as params

db = params.testdir + 'storage/tmpdb'
c1 = params.testdir + 'storage/Conf.csv'


def go(t):
    o = option.Option(**{'handle': t, 'type': t})
    o.validate()
    return o


class SqlStore(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        if os.path.exists(db):
            os.remove(db)
        cls.conn = sqlite3.connect(db, detect_types=sqlite3.PARSE_DECLTYPES)
        st0 = option.CsvStore(kid='/base/')
        st0.merge_file(c1)
        st0.validate()
        cls.desc = st0.desc

    def test_get_typed_cols(self):
        print get_typed_cols(go('Integer'))
        print get_typed_cols(go('String'))
        print get_typed_cols(go('Point'))
        print get_typed_cols(go('Role'))
        print get_typed_cols(go('RoleIO'))
        print get_typed_cols(go('Log'))
        print get_typed_cols(go('Meta'))

    def test_get_insert_cmd(self):
        print get_insert_cmd(go('Integer'), base_col_def)
        print get_insert_cmd(go('String'), base_col_def)
        print get_insert_cmd(go('Point'), base_col_def)
        print get_insert_cmd(go('Role'), base_col_def)
        print get_insert_cmd(go('RoleIO'), base_col_def)
        print get_insert_cmd(go('Log'), base_col_def)
        print get_insert_cmd(go('Meta'), base_col_def)

    def test_column_definition(self):
        s = option.SqlStore()
        print s.column_definition(go('Integer'))[1]
        print s.column_definition(go('String'))[1]
        print s.column_definition(go('Point'))[1]
        print s.column_definition(go('Role'))[1]
        print s.column_definition(go('RoleIO'))[1]
        print s.column_definition(go('Log'))[1]
        print s.column_definition(go('Meta'))[1]

    def test_write_desc(self):
        s = option.SqlStore()
        s.cursor = self.conn.cursor()
        s.write_desc(self.desc)
        print 'READING'
        r = s.read_tree()
        print r
        print 'print tree\n', print_tree(r)
        print 'WRITING AGAIN'
        s.write_tree(r)
        print "READING AGAIN"
        r = s.read_tree()
        print r
        print 'print tree2\n', print_tree(r)


#	@unittest.skip('')
    def test_tables(self):
        st0 = option.CsvStore(kid='ciao')
        st0.merge_file(c1)
        st = option.SqlStore(kid='ciao')
        st.desc = st0.desc
        k0 = set(st.desc.iterkeys())
        cursor = self.conn.cursor()
        st.write_table(cursor, 'conf1')
        self.conn.commit()
        cursor.execute('select handle from conf1')
        r = cursor.fetchall()
        k1 = set([eval(k[0]) for k in r])
        self.assertEqual(k0, k1)

        st2 = option.SqlStore(kid='ciao')
        st2.read_table(cursor, 'conf1')
        self.assertEqual(st.desc, st2.desc)


if __name__ == "__main__":
    unittest.main()
