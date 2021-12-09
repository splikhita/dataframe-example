from dataclasses import dataclass

#The current, modern way to do this (as of Python 3.7) is with a data class.
#Before Python 3.7, there's collections.namedtuple:


@dataclass(frozen=True)
class Product(object):
    product: str
    category: str
    revenue: int
