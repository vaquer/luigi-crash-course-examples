import os
import csv
import json
import luigi
from luigi.contrib.postgres import CopyToTable, PostgresTarget
import requests


class DownloadProduct(luigi.Task):
    producto = luigi.Parameter()

    def run(self):
        page = 1
        must_continue = True
        list_product = []

        while must_continue:
            print("Peticion al API pagina: ", str(page))
            self.set_status_message("Peticion al API QQP, producto: {} pagina: {}".format(self.producto, str(page)))

            response = requests.get('https://api.datos.gob.mx/v1/profeco.precios', params={'producto': self.producto, 'page': str(page)})
            print("Respuesta del servidor", response.status_code)
            if response.status_code == 200:
                json_response = response.json().get('results', [])
                must_continue = len(json_response) > 0

                if must_continue:
                    list_product.extend(json_response)
                    page += 1

        if len(list_product) > 0:
            with self.output().open('w') as json_file:
                json.dump(list_product, json_file)

    def output(self):
        return luigi.LocalTarget('/tmp/qqp/{}/data.json'.format(self.producto))


class ConvertJSONToCSV(luigi.Task):
    producto = luigi.Parameter()

    def requires(self):
        return DownloadProduct(self.producto)

    def run(self):
        with self.input().open('r') as json_file:
            json_product = json.load(json_file)

        print(len(json_product))
        headers = json_product[0].keys()

        with self.output().open('w') as csv_file:
            writer = csv.writer(csv_file, delimiter='|', quotechar='"')

            for produc in json_product:
                writer.writerow(list(produc.values()))

    def output(self):
        return luigi.LocalTarget('/tmp/qqp/{0}/data.csv'.format(self.producto))


class InsertDataInDataBase(CopyToTable):
    producto = luigi.Parameter()
    host = os.environ.get('DB_HOST', '0.0.0.0:5433')
    database = os.environ.get('DB_DATABASE', 'QQP')
    user = os.environ.get('DB_USER', 'QQP')
    password = os.environ.get('DB_PASSWORD', 'q1q2p')
    port = os.environ.get('DB_PORT', 5433)
    table = os.environ.get('DB_TABLE','PRODUCTO')
    columns = [
        'id',
        'product',
        'presentacion',
        'marca',
        'categoria',
        'catalogo',
        'precio',
        'fechaRegistro',
        'cadenaComercial',
        'giro',
        'rfc',
        'razonSocial',
        'nombreComercial',
        'direccion',
        'estado',
        'municipio',
        'latitud',
        'longitud'
    ]

    def requires(self):
        return ConvertJSONToCSV(self.producto)


# class AggretateByState(PostgresQuery):
#     producto = luigi.Parameter()

#     # def requires(self):
#     #     return ConvertJSONToCSV(self.producto)


class StartPipeline(luigi.Task):
    producto = luigi.Parameter()

    def requires(self):
        return InsertDataInDataBase(self.producto)