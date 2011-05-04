from cPickle import dumps, loads
from nose.tools import ok_, eq_

from ..namespace import *


def test_basic():
    N = NamespaceNode()
    yield eq_, 'foo' in N, False
    N.foo.bar
    yield eq_, 'foo' in N, False
    N.foo.bar.baz = 3
    yield eq_, 'foo' in N, True
    yield ok_, isinstance(N.foo.bar, NamespaceNode)
    yield eq_, dir(N), ['foo']


def test_pickling():
    N = NamespaceNode()
    N.foo.bar.baz = 3    
    M = loads(dumps(N))
    yield eq_, M.foo.bar.baz, 3
