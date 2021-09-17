"""A module for data models related to asthma case data.

Each document in the database's Region collection contains an embedded document
describing the number of asthma cases in that region. This module defines that embedded
document via the AsthmaData class. Refer to the docstring of the AsthmaData class for
a more in-depth description of the class' fields.

All data regarding asthma cases counts is pulled from the American Lung Association's
"State of the Air" project. Referencing their `methodology`_ webpage might be helpful step in
understanding our schema's fields, structure, and enum choices.

.. methodology:
    https://www.stateoftheair.org/about/methodology-and-acknowledgements.html

"""

from mongoengine import (EmbeddedDocument, FloatField, BooleanField)


class AsthmaData(EmbeddedDocument):
    population = FloatField()
    adult = FloatField()
    child = FloatField()
    non_white = FloatField()
    poverty = FloatField()
    extrapolated = BooleanField(required=True)

    def __repr__(self):
        return "<Asthma(adult='%d', child='%d')>" % (self.adult, self.child)
