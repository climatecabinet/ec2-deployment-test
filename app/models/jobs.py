from mongoengine import (EmbeddedDocument, IntField, FloatField,
                         EmbeddedDocumentField, BooleanField,
                         DynamicEmbeddedDocument)


class JobsStat(DynamicEmbeddedDocument):
    residential = FloatField()
    commercial = FloatField()
    utility = FloatField()
    total = FloatField()

    def __repr__(self):
        return (f"<JobsStat(res={self.residential}, com={self.commercial}, "
                f"utl={self.utility}, total={self.total})")


class JobsCounts(EmbeddedDocument):
    solar = IntField()
    wind = IntField()
    energy = IntField()
    total = IntField()

    def __repr__(self):
        return (f"<JobsCounts(solar={self.solar}, wind={self.wind}, "
                f"energy={self.energy}, total={self.total})")


class JobsData(EmbeddedDocument):
    counts = EmbeddedDocumentField(JobsCounts)
    perc_of_state_jobs = FloatField()
    mwh_invested = EmbeddedDocumentField(JobsStat)
    dollars_invested = EmbeddedDocumentField(JobsStat)
    installations_count = EmbeddedDocumentField(JobsStat)
    mw_capacity = EmbeddedDocumentField(JobsStat)
    extrapolated = BooleanField(required=True)

    def __repr__(self):
        return (f"<Jobs(% of state's jobs={self.perc_of_state_jobs}, "
                f"total dollars invested={self.dollars_invested.total})>")
