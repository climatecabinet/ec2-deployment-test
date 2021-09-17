"""Defines the Region ODM class and related embedded ODM classes.

"""
from datetime import datetime
from enum import Enum
from mongoengine import (
    Document, StringField, IntField, EmbeddedDocument, EmbeddedDocumentField,
    EmbeddedDocumentListField, SortedListField, BooleanField, LazyReferenceField, ListField,
    ReferenceField, FloatField, DateTimeField, URLField
)
from mongoengine.base import GeoJsonBaseField


class RegionShapeField(GeoJsonBaseField):
    _type = 'RegionShape'

    def validate(self, value, r_type=None):
        """Validate the GeoJson object based on its type."""
        if isinstance(value, dict):
            if set(value.keys()) == {"type", "coordinates"}:
                if value["type"].lower() not in ['polygon', 'multipolygon']:
                    self.error(
                        f'{self._name} type must be "Polygon" or "MultiPolygon", '
                        f'not {value["type"]}'
                    )
                return self.validate(value["coordinates"], r_type=value["type"])
            else:
                self.error(
                    "%s can only accept a valid GeoJson dictionary"
                    " or lists of (x, y)" % self._name
                )
                return
        elif not isinstance(value, (list, tuple)):
            self.error("%s can only accept lists of [x, y]" % self._name)
            return

        validate = getattr(self, "_validate_%s" % r_type.lower())
        error = validate(value)
        if error:
            self.error(error)


class Shape(Document):
    year = IntField(required=True)
    shape = RegionShapeField(required=True)
    state_abbr = StringField(required=True, max_length=2, min_length=2)
    geoid = StringField(required=True)
    ccid = StringField(required=True)
    name = StringField(required=True)
    region = ReferenceField('Region', required=True)
    land_area = FloatField(required=True)

    date_modified = DateTimeField(default=datetime.utcnow)

    meta = {
        'indexes': [
            {
                'fields': ['ccid', 'year'],
                'unique': True
            },
            'state_abbr',
            '(shape'
        ]
    }

    def __repr__(self):
        return f"<Shape(name='{self.name}', ccid='{self.ccid}')>"


class RegionShape(EmbeddedDocument):
    """Stores the geojson shape for a region"""
    year = IntField(required=True)
    shape = ReferenceField(Shape, required=True)

    def __repr__(self):
        return f"<RegionShape(year={self.year})>"


class RegionFragment(EmbeddedDocument):
    """Stores a fragment of a region"""
    region = LazyReferenceField('Region', required=True)
    population = IntField(required=True)
    perc_of_whole = FloatField(required=True)

    def __repr__(self):
        return f"<RegionFragment(ccid='{self.region.fetch().ccid}'," \
               f" perc='{self.perc_of_whole}')>"


class Incumbent(EmbeddedDocument):
    name = StringField(required=True)
    rep = ReferenceField('Representative', required=True)


class EnvironmentalOrg(EmbeddedDocument):
    """ The name and website of an environmental org. whose scorecards we use.
    """
    name = StringField(required=True)
    website = URLField(required=True)


class Region(Document):
    """Base class for all region-style documents"""
    state_fips = StringField(required=True)
    state_abbr = StringField(required=True, max_length=2, min_length=2)
    geoid = StringField(required=True)
    ccid = StringField(required=True, unique=True)
    name = StringField(required=True)
    shapes = SortedListField(
        EmbeddedDocumentField(RegionShape), required=True, ordering="year", reverse=True
    )
    fragments = ListField(EmbeddedDocumentField(RegionFragment))
    asthma = EmbeddedDocumentField('AsthmaData')
    polling = EmbeddedDocumentField('PollingData')
    jobs = EmbeddedDocumentField('JobsData')

    date_modified = DateTimeField(default=datetime.utcnow)

    meta = {
        'indexes': [
            {
                'fields': ['geoid'],
                'unique': True
            },
            '_cls',
            'state_abbr',
        ],
        'allow_inheritance': True,
    }

    def __repr__(self):
        return f"<Region(name='{self.name}', ccid='{self.ccid}')>"

    def clean(self):
        # Anytime save() is called, make sure the date_modified field updates
        self.date_modified = datetime.utcnow

    def extrapolate_count(self, target_cls, frag_type, doc_attr, omit=[]):
        """Extrapolates region-specific data from data of intersecting regions
        using the population-based fragments list.

        Creates a new instance of the target embedded document, populated with
        weighted averages from similar data of intersecting regions of type
        "frag_type". Designed to be used populate data fields for regions
        without direct data, but which has intersecting regions with direct
        data.

        Args:
            target (EmbeddedDocument): The class of the embedded document to
                extrapolate.
            frag_type (RegionType): The type of intersecting region that has
                non-extrapolated data. The data from intersecting regions of
                this type will be used in the weighted averaging.
            doc_attr (str): The attribute path to the location of the embedded
                document in the regions that data will be extracted from.
            year (int, optional): the year to calculate region intersection.
                Defaults to the current year.
            omit ([str], optional): a list of field names inside the target_cls
                that should be skipped when extrapolating data for the new
                class instance.

        Returns:
            EmbeddedDocument: The new embedded document with extrapolated data
        """
        target = target_cls()

        for f in self.fragments:
            # if the type of region creating this fragment with self's region
            # isn't the region type specified by frag_type, skip it
            if Region.objects.only('_cls').get(id=f.region.id)._cls != frag_type.cls_name:
                continue

            f_reg = Region.objects.only('fragments', 'ccid', doc_attr).get(id=f.region.id)

            if (source_emb_doc := getattr(f_reg, doc_attr)).extrapolated:
                raise Exception(
                    f"Extrapolation error - could not extrapolate {target_cls} data for "
                    f"{self} using intersecting {frag_type.name} regions, since those "
                    f"intersecting regions have extrapolated data themselves. Make sure "
                    f"extrapolated data is sourced from non-extrapolated regions."
                )

            frag_of_self_from_source = list(filter(
                lambda f: f.region.id == self.id, f_reg.fragments
            ))[0]

            source_perc_of_whole = frag_of_self_from_source.perc_of_whole

            for field in target._data.keys():
                if field not in omit:
                    curr = getattr(target, field) if getattr(target, field) else 0
                    new_val = curr + getattr(source_emb_doc, field)*source_perc_of_whole
                    setattr(target, field, new_val)

        target.extrapolated = True
        return target

    def extrapolate_weighted_average(self, target_cls, frag_type, doc_attr, omit=[]):
        target = target_cls()

        for f in self.fragments:
            # if the type of region creating this fragment with self's region
            # isn't the region type specified by frag_type, skip it
            if Region.objects.only('_cls').get(id=f.region.id)._cls != frag_type.cls_name:
                continue

            f_reg = Region.objects.only('fragments', 'ccid', doc_attr).get(id=f.region.id)

            if (source_emb_doc := getattr(f_reg, doc_attr)).extrapolated:
                raise Exception(
                    f"Extrapolation error - could not extrapolate {target_cls} data for "
                    f"{self} using intersecting {frag_type.name} regions, since those "
                    f"intersecting regions have extrapolated data themselves. Make sure "
                    f"extrapolated data is sourced from non-extrapolated regions."
                )

            for field in target._fields.keys():
                if field not in omit:
                    curr = getattr(target, field) if getattr(target, field) else 0
                    new_val = curr + getattr(source_emb_doc, field)*f.perc_of_whole
                    setattr(target, field, new_val)

        target.extrapolated = True
        return target


class State(Region):
    environmental_organizations = EmbeddedDocumentListField(EnvironmentalOrg)
    """[EnvironmentalOrg]: A list of the environmental organizations whose
                           scorecards we consider in our curation data. Each
                           EmbeddedDocument includes the organization's name
                           and website.
    """

    def set_region_specific_fields(self, props):
        pass

    def __repr__(self):
        return "<State(name='%s', ccid='%s')>" % (self.name, self.ccid)


class County(Region):
    county_fips = StringField()
    county_gnis = StringField()
    population = FloatField()
    is_independent_city = BooleanField(required=True)
    """bool: A true/false flag denoting independent cities stored in the county collection.
             Independent cities (is.gd/LpK9Xx) act like counties in function, but are not
             technically counties, and may have the exact same name as a county in the same
             state (ie - Fairfax County vs. Fairfax City in VA).
    """

    def __repr__(self):
        return "<County(name='%s', ccid='%s')>" % (self.name, self.ccid)


class District(Region):
    district_no = StringField(required=True)
    district_type = StringField(required=True)
    shortcode = StringField(required=True)
    incumbents = EmbeddedDocumentListField(Incumbent)
    presidential_elections = SortedListField(
        EmbeddedDocumentField('PresidentialElection'),
        ordering="year",
        reverse=True
    )

    meta = {'allow_inheritance': True}

    def __repr__(self):
        return f"<District(name='{self.name}', ccid='{self.ccid}')>"


class CongressionalDistrict(District):
    district_session = StringField()

    def __repr__(self):
        return f"<CongrDist(name='{self.name}', ccid='{self.ccid}')>"


class StateLegDistUpper(District):
    leg_year = IntField()

    def __repr__(self):
        return f"<SLDU(name='{self.name}', ccid='{self.ccid}')>"


class StateLegDistLower(District):
    leg_year = IntField()

    def __repr__(self):
        return f"<SLDL(name='{self.name}', ccid='{self.ccid}')>"


class RegionType(Enum):
    STATE = (
        "State",
        State,
        "G4000",
        "Region.State",
        "state"
    )
    COUNTY = (
        "County",
        County,
        "G4020",
        "Region.County",
        "county"
    )
    CONGR = (
        "Congressional District",
        CongressionalDistrict,
        "G5200",
        "Region.District.CongressionalDistrict",
        "congressional district"
    )
    SLDU = (
        "Upper State Legislative District",
        StateLegDistUpper,
        "G5210",
        "Region.District.StateLegDistUpper",
        "state legislative district (upper chamber)",
        "S"

    )
    SLDL = (
        "Lower State Legislative District",
        StateLegDistLower,
        "G5220",
        "Region.District.StateLegDistLower",
        "state legislative district (lower chamber)",
        "H"
    )

    def __init__(
        self,
        full,
        cls,
        maf,
        cls_name,
        census,
        dist_abbr=None
    ):
        self.full = full
        self.cls = cls
        self.maf = maf
        self.cls_name = cls_name
        self.census = census
        self.dist_abbr = dist_abbr

    @classmethod
    def fuzzy_cast(cls, type_arg):
        reg_tuples_to_instances = {rt[-1].value: rt[-1] for rt in dict(cls.__members__).items()}
        found = None

        for reg_tuple in reg_tuples_to_instances.keys():
            if type_arg in reg_tuple:

                if found:
                    raise ValueError(f"RegionType Enum Error - fuzzy_cast argument {type_arg} "
                                     f"matches with multiple RegionType options. Please change "
                                     f"RegionType options to prevent collision like this.")

                found = reg_tuples_to_instances[reg_tuple]

        if not found:
            raise ValueError(f"RegionType Enum Error - unable to parse argument {type_arg} as a "
                             f"valid RegionType option.")

        return found
