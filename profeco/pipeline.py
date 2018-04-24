import luigi

class InsertDataInDataBase(luigi.Postgres):
    producto = luigi.Paramter()

    def requires(self):
        return MergeAgregations(self.producto)


class MergeAgregations(luigi.Postgres):
    producto = luigi.Paramter()

    def requires(self):
        return MergeAgregations(self.producto)


class AggretateByState(luigi.Task):
    producto = luigi.Paramter()

    def requires(self):
        return MergeAgregations(self.producto)


