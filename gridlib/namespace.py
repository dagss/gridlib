"""Utilities for constructing namespaces, a la a simpler
pygr.worldbase.
"""

class NamespaceNode(object):
    def __init__(self, parent=None):
        self.__dict__['_children'] = {}
        self.__dict__['_parent'] = parent
        self.__dict__['_empty'] = True

    def __getattr__(self, childname):
        if childname.startswith('__'): # needed for, e.g., pickling
            raise AttributeError()
        children = self._children
        x = children.get(childname, None)
        if x is None:
            x = NamespaceNode(self)
            children[childname] = x
        return x

    def __setattr__(self, childname, value):
        if isinstance(value, NamespaceNode):
            if value._parent is not None:
                raise ValueError('node already belongs in another tree')
            value.__dict__['_parent'] = self
        self._children[childname] = value
        node = self
        while node is not None and node._empty:
            node.__dict__['_empty'] = False
            node = node._parent

    def __contains__(self, childname):
        return (childname in self._children and
                not self._children[childname]._empty)

    def __dir__(self):
        return [key for key, child in self._children.iteritems()
                if not (isinstance(child, NamespaceNode) and child._empty)]


    
