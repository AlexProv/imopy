def injectArguments(inFunction):
    """
    Decorator injecting arguments of a method as attributes

    Found here: http://code.activestate.com/recipes/577382-keyword-argument-injection-with-python-decorators/

    """

    def outFunction(*args, **kwargs):
        _self = args[0]
        _self.__dict__.update(kwargs)

        # Get all of argument's names of the inFunction
        _total_names = inFunction.__code__.co_varnames[1:inFunction.__code__.co_argcount]
        # Get all of the values
        _values = args[1:]
        # Get only the names that don't belong to kwargs
        _names = [n for n in _total_names if not n in kwargs]

        # Match argument names with values and update __dict__
        _self.__dict__.update(zip(_names,_values))

        # Add default value for non-specified arguments
        nb_defaults = len(_names) - len(_values)
        _self.__dict__.update(zip(_names[-nb_defaults:], inFunction.__defaults__[-nb_defaults:]))

        return inFunction(*args,**kwargs)

    return outFunction



class House(object):
    @injectArguments
    def __init__(self, marketDate=None, sellingPrice=None, salesUrl=None, salesDate=None,
                finalPrice=None, city=None, neighbourhood=None, postCode=None, street=None,
                civicNumber=None, yearBuilt=None, lat=None, lng=None,
                basement=None, rooms=None, houseSize=None, bedrooms=None,
                bedroomsAbove=None, bedroomsBasement=None, bathrooms=None, toilet=None, features=None,
                Sold=False):
       pass

    @staticmethod
    def from_dict(source):
        return House(source)

    def to_dict(self):
        return self.__dict__

    def __repr__(self):
        return self.to_dict()
