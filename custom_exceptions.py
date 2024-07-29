
"""Module to write custom exceptions"""


class UnknownRetailer(Exception):
    """When unidentified retailer is passed"""
    pass


class MissingValue(Exception):
    """When a required value is missing"""
    pass
