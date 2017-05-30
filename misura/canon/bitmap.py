#!/usr/bin/python
# -*- coding: utf-8 -*-
"""Decompressione immagini bitmap utilizzate da Misura3"""
# Ispirato a:
# http://pseentertainmentcorp.com/smf/index.php?topic=2034.0

import struct
try:
    from cStringIO import StringIO
except:
    from io import StringIO

bitmapHeader_fmt = '<BBLHHLLLLHHLLLLLL'
bitmapHeader_len = struct.calcsize(bitmapHeader_fmt)
colormap_fmt = '<' + 'I' * 256
colormap_len = struct.calcsize(colormap_fmt)
colormap_bin = struct.pack(colormap_fmt, *tuple(range(0, 256)))
# Coppie Colore, Conteggio compresse
inbit_fmt = '<BB'
inbit_len = struct.calcsize(inbit_fmt)
# Colori decompressi
outbit_fmt = '<B'
outbit_len = struct.calcsize(outbit_fmt)
# Bitmap header positions
MN1 = 0
MN2 = 1
FILESIZE = 2
UNDEF1 = 3
UNDEF2 = 4
OFFSET = 5
HEADERLENGTH = 6
WIDTH = 7
HEIGHT = 8
COLORPLANES = 9
COLORDEPTH = 10
COMPRESSION = 11
IMAGESIZE = 12
HRES = 13
VRES = 14
PALETTE = 15
IMPORTANTCOLORS = 16


outHeader = [66,  # mn1
             77,  # mn2
             0,  # filesize
             0,  # u1
             0,  # u2
             54,  # offset
             40,  # hl
             640,  # w
             480,  # h
             1,  # cp
             8,  # cd
             0,  # compress
             0,  # imgsize
             0,  # hres
             0,  # vres
             256,  # palette
             0]  # impc


def convertBitmap(img):
    img.seek(0)
    # Leggo header
    fh = struct.unpack(bitmapHeader_fmt, img.read(bitmapHeader_len))
    outH = list(outHeader[:])
    for val in WIDTH, HEIGHT:
        outH[val] = fh[val]
    # Leggo la mappa dei colori
    cmap = img.read(colormap_len)
    i = -1
    out = ''
    while True:
        i += 1
        r = img.read(inbit_len)
        if len(r) != inbit_len:
            break
        n, c = struct.unpack(inbit_fmt, r)
        for j in range(n):
            out += struct.pack(outbit_fmt, c)

    outH[IMAGESIZE] = len(out)
    outH[OFFSET] = bitmapHeader_len + colormap_len
    outH[FILESIZE] = outH[IMAGESIZE] + outH[OFFSET]
    out = struct.pack(bitmapHeader_fmt, *tuple(outH)) + cmap + out
    return out


def decompress(img):
    if img[:2] != 'IM':  # MI is an old id
        return img
    img = StringIO(img)
    img.seek(0)
    return convertBitmap(img)
