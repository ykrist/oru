import numpy as np
from functools import wraps

def P(a,b):
    return np.array([a,b])

def perp(p):
    return P(-p[0],p[1])

def unit(p):
    return p/np.linalg.norm(p)

def to_polar(p):
    return np.linalg.norm(p),np.arctan2(p[1],p[0])


class Interval:
    def __init__(self,a,b):
        assert a < b
        self.start = a
        self.end = b

    def __contains__(self, val):
        return self.start <= val <= self.end

    def __matmul__(self, other):
        return Rectangle(self.start, other.start, self.end, other.end)

class Rectangle:
    def __init__(self, x1,y1,x2=None,y2=None):
        if x2 == y2 == None:
            x2,y2 = x1,y1
            x1 = y1 = 0

        self.x_bounds = Interval(x1, x2)
        self.y_bounds = Interval(y1, y2)

    def __contains__(self, point):
        return point[0] in self.x_bounds and point[1] in self.y_bounds

    @property
    def start(self):
        return np.array([self.x_bounds.start, self.y_bounds.start])

    @property
    def end(self):
        return np.array([self.x_bounds.end, self.y_bounds.end])

class Circle:
    def __init__(self, center=P(0,0), radius=1):
        self.center=center
        self.radius=radius

    def __contains__(self, point):
        return np.linalg.norm(point-self.center) <= self.radius

    def __and__(self, other):
        d = np.linalg.norm(other.center - self.center)

        if other.radius + self.radius < d  or d <= abs(other.radius-self.radius):
            return []

        a =  (d**2 + self.radius**2 - other.radius**2 ) / (2*d)

        h = (self.radius**2 - a**2)**0.5

        p = self.center + a*(other.center - self.center)/d

        v = P(h*(other.center[1]-self.center[1])/d, - h*(other.center[0]-self.center[0])/d)

        if np.allclose(v, 0):
            return [p]

        return [p+v, p-v]



def check_domain(func):
    @wraps(func)
    def wrapper(self, arg, **kwargs):
        if arg not in self.domain:
            raise ValueError(f"{str(arg)} is not in the domain.")
        return func(arg, **kwargs)
    return wrapper

class BaseTransform:
    pass

class LinearTransform(BaseTransform):
    def __init__(self, src_interval : Interval, dest_interval : Interval):
        a,b = src_interval.start, src_interval.end
        c,d = dest_interval.start, dest_interval.end
        self.scale = (d-c)/(b-a)
        self.offset = (b*c-a*d)/(b-a)
        self.domain = src_interval

    @check_domain
    def __call__(self, val):
        assert val in self.domain
        return self.scale*val + self.offset

class RectangularTransform(BaseTransform):
    def __init__(self, source_rectangle : Rectangle, dest_rectangle : Rectangle):
        ax1,ay1 = source_rectangle.start
        ax2,ay2 = source_rectangle.end
        bx1,by1 = dest_rectangle.start
        bx2,by2 = dest_rectangle.end
        x_transform = LinearTransform((ax1,ax2), (bx1,bx2))
        y_transform = LinearTransform((ay1,ay2), (by1,by2))

        self.scale = np.array([x_transform.scale, y_transform.scale])
        self.offset = np.array([x_transform.offset, y_transform.offset])
        self.domain = source_rectangle

    @check_domain
    def __call__(self, point):
        return self.scale*point + self.offset




