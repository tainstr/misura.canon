#!/usr/bin/python
# -*- coding: utf-8 -*-
import numpy as np

det = lambda m: np.linalg.det(np.matrix(m))


def circle(a, b, c):
    """Calculate circle radius given three points (xa,ya),(xb,yb),(xc,yc)"""
    # following http://mathworld.wolfram.com/Circle.html
    A = det([[a[0], a[1], 1],
             [b[0], b[1], 1],
             [c[0], c[1], 1]])
    if abs(A) < 10**-8:
        return 0, 0, 0
    sa = a[0]**2 + a[1]**2
    sb = b[0]**2 + b[1]**2
    sc = c[0]**2 + c[1]**2
    D = -det([[sa, a[1], 1],
              [sb, b[1], 1],
              [sc, c[1], 1]])
    E = det([[sa, a[0], 1],
             [sb, b[0], 1],
             [sc, c[0], 1]])
    F = -det([[sa, a[0], a[1]],
              [sb, b[0], b[1]],
              [sc, c[0], c[1]]])
    x = -D / (2 * A)
    y = -E / (2 * A)
    G = (D**2 + E**2) / (4 * (A**2))
    r = np.sqrt(G - (F / A))
    if r > 10**12:
        return 0, 0, 0
    return float(r), float(x), float(y)

def absolute_flex(middle_delta, right_delta, left_delta, 
                  right_optics_position, left_optics_position):
    # Concyclic points
    left_point   =    [-left_optics_position  , left_delta]
    center_point =  [0                      , middle_delta]
    right_point  =   [right_optics_position  , right_delta]
    smp = {}
    radius, x_center, y_center = circle(left_point, center_point, right_point)
    smp['radius'] = radius
    if radius == 0:
        smp['d'] = 0
        return smp
    
    # Calculate the theoretical point of maximum flexion (which may not be equal to d)
    # Length of the chord is the sum of R/L distances from middle cam
    q = (right_optics_position + left_optics_position) / 2.
    # Angle associated with half-chord
    ang = np.arcsin(q / radius)
    # Maximum circle-chord distance
    d1 = radius * (1 - np.cos(ang))
    # Apply the sign of the center y relative to the middle y
    smp['d'] = - np.sign(y_center - middle_delta) * abs(float(d1))
    return smp


def syn(a, b, c, step=1):
    """Create a circle (x,y) sequence passing through a,b,c"""
    # Find circle radius
    r, Ox, Oy = circle(a, b, c)
    sg = np.sign(step)
    y = a[1]
    vr = []
    vl = []
    vxr = []
    vxl = []
    vy = []
    while sg * (b[1] - y) >= 0:
        y1 = sg * (Oy - y)
        x1 = r * np.cos(np.arcsin(y1 / r))
        vxr.append(Ox - x1)
        vxl.append(Ox + x1)
        vy.append(y)
        vl.append((vxl[-1], y))
        vr.append((vxr[-1], y))
        y += step
    y = vy + vy[::-1]
    if sg > 0:
        x = vxl + vxr[::-1]
        v = vl + vr[::-1]
    else:
        x = vxr + vxl[::-1]
        v = vr + vl[::-1]
    print(v)
    return x, y, v


if __name__ == '__main__':
    import pylab
#	x,y,v=syn((-1,0),(0,1),(1,0),0.01)
    x, y, v = syn([33., 66.], [49., 33.], [66., 66.], -1)
    pylab.plot(x, y, 'r-')
    pylab.show()
