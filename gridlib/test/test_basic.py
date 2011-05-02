from nose.tools import eq_, ok_
from .. import versioned

def test_versioned():
    @versioned
    def dec_no_parens(x):
        pass

    @versioned(4, deps=False, ignore=['y'])
    def f(x, y):
        return x**2 + y

    @versioned()
    def g(x):
        return x
    
    yield ok_, hasattr(dec_no_parens, 'version_info')

    yield eq_, 2, f(1, 1)
    yield ok_, hasattr(f, 'version_info')
    if hasattr(f, 'version_info'):
        d = dict(f.version_info)
        yield eq_, len(d['digest']), 20
        del d['digest']
        yield eq_, d, dict(version='4', ignore_deps=True, ignore_args=('y',))

    yield ok_, hasattr(g, 'version_info')
    if hasattr(g, 'version_info'):
        d = dict(g.version_info)
        yield eq_, len(d['digest']), 20
        del d['digest']
        yield eq_, d, dict(version='e7ay4y4u2drtcfr5kf5ra6jkpydjhomv',
                           ignore_deps=False, ignore_args=())


