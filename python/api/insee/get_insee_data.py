import requests
import pandas as pd
import time
import os
import json
import logging
from google.oauth2 import service_account
from google.cloud import bigquery
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import random
import traceback

# Configuration du logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class UniteLegaleExtractor:
    def __init__(self, table_id, credentials_path):
        self.table_id = table_id
        self.credentials = service_account.Credentials.from_service_account_file(
            credentials_path, scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        self.bigquery_client = bigquery.Client(credentials=self.credentials, project=self.table_id.split(".")[0])
        self.api_url = "https://api.insee.fr/entreprises/sirene/V3.11/siren"
        self.headers = {"Authorization": "Bearer_Token"}
        self.retry_attempts = 0
        self.max_retries = 5

    def create_retry_session(self, retries=3, backoff_factor=0.3, status_forcelist=(500, 502, 504)):
        session = requests.Session()
        retry = Retry(
            total=retries,
            read=retries,
            connect=retries,
            backoff_factor=backoff_factor,
            status_forcelist=status_forcelist,
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def wait_for_rate_limit(self, calls, period=60, max_calls=30):
        now = time.time()
        calls = [call for call in calls if now - call < period]
        if len(calls) >= max_calls:
            sleep_time = period - (now - calls[0])
            if sleep_time > 0:
                logger.info(f"Rate limit reached. Sleeping for {sleep_time} seconds.")
                time.sleep(sleep_time)
        calls.append(now)
        return calls

    def wait_with_backoff(self, attempt):
        sleep_time = (2**attempt) + random.uniform(0, 1)
        logger.info(f"Backing off for {sleep_time} seconds.")
        time.sleep(sleep_time)

    def prepare_for_bigquery(self, df):
        expected_columns = [
            'siren', 'statutDiffusionUniteLegale', 'dateCreationUniteLegale', 'sigleUniteLegale', 'sexeUniteLegale',
            'prenom1UniteLegale', 'prenom2UniteLegale', 'prenom3UniteLegale', 'prenom4UniteLegale', 'prenomUsuelUniteLegale',
            'pseudonymeUniteLegale', 'identifiantAssociationUniteLegale', 'trancheEffectifsUniteLegale', 'anneeEffectifsUniteLegale',
            'dateDernierTraitementUniteLegale', 'nombrePeriodesUniteLegale', 'categorieEntreprise', 'anneeCategorieEntreprise',
            'periodesUniteLegale', 'date_extraction'
        ]
        df_filtered = pd.DataFrame({col: df[col] if col in df else None for col in expected_columns})
        df_filtered['nombrePeriodesUniteLegale'] = pd.to_numeric(df_filtered['nombrePeriodesUniteLegale'], errors='coerce')
        for col in df_filtered.columns:
            if df_filtered[col].dtype == "object":
                df_filtered[col] = df_filtered[col].apply(lambda x: json.dumps(x) if isinstance(x, (list, dict)) else x)
        return df_filtered.where(pd.notnull(df_filtered), None)

    def get_last_update_date(self):
        query = f"SELECT MAX(dateDernierTraitementUniteLegale) as max_date FROM `{self.table_id}`"
        query_job = self.bigquery_client.query(query)
        results = query_job.result()
        for row in results:
            return datetime.strptime(row.max_date, "%Y-%m-%dT%H:%M:%S.%f").strftime("%Y-%m-%d") if row.max_date else "1970-01-01"

    def get_and_send_uniteLegale(self, batch_size=1000, last_update_date=None):
        cursor = "*"
        api_calls = []
        total_sent = 0
        current_batch = []
        session = self.create_retry_session()
        total_results = None
        lot_number = 1
        retry_attempts = 0

        logger.info("=== Démarrage du script ===")
        logger.info(f"Récupération du nombre total d'entreprises depuis la date : {last_update_date}")

        while True:
            api_calls = self.wait_for_rate_limit(api_calls)
            params = {
                "q": f"dateDernierTraitementUniteLegale:[{last_update_date} TO *]",
                "nombre": 1000,
                "curseur": cursor,
            }

            try:
                response = session.get(self.api_url, headers=self.headers, params=params, timeout=30)
                response.raise_for_status()
            except requests.exceptions.RequestException as e:
                logger.error(f"Request error: {e}")
                if response is not None:
                    logger.error(f"Request URL: {response.url}")
                self.wait_with_backoff(1)
                continue

            if response.status_code == 200:
                data = response.json()
                if total_results is None:
                    total_results = data.get("header", {}).get("total", 0)
                    logger.info(f"Nombre total d'entreprises à traiter : {total_results}")

                unites_legales = data.get("unitesLegales", [])
                if not unites_legales:
                    logger.info("Aucune unité légale trouvée, fin de la récupération des données.")
                    break

                for unite in unites_legales:
                    unite["date_extraction"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    current_batch.append(unite)

                while len(current_batch) >= batch_size:
                    batch_to_send = current_batch[:batch_size]
                    current_batch = current_batch[batch_size:]
                    self.send_batch_to_bigquery(batch_to_send, lot_number)
                    total_sent += len(batch_to_send)
                    lot_number += 1
            elif response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 60))
                logger.info(f"Limite d'API atteinte. Pause de {retry_after} secondes...")
                time.sleep(retry_after)
            else:
                logger.error(f"Request error: {response.status_code}")
                self.wait_with_backoff(1)
            cursor = data.get("header", {}).get("curseurSuivant")
            if not cursor:
                logger.info("Aucun curseur suivant trouvé, fin de la récupération des données.")
                break

        if current_batch:
            self.send_batch_to_bigquery(current_batch, lot_number)

        logger.info(f"\n✓ Traitement terminé! Total traité: {total_sent} entreprises")

    def send_batch_to_bigquery(self, batch, lot_number):
        df_batch = pd.DataFrame(batch)
        df_prepared = self.prepare_for_bigquery(df_batch)
        retry_attempts = 0
        while retry_attempts < self.max_retries:
            try:
                job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND", autodetect=True)
                job = self.bigquery_client.load_table_from_dataframe(df_prepared, self.table_id, job_config=job_config)
                job.result()
                logger.info(f"✓ Lot {lot_number} envoyé avec succès.")
                retry_attempts = 0
                break
            except Exception as e:
                retry_attempts += 1
                logger.error(f"Erreur lors de l'envoi du lot {lot_number}: {e}, tentative {retry_attempts}/{self.max_retries}")
                time.sleep(60)
        if retry_attempts == self.max_retries:
            logger.error("Échec d'envoi du lot après plusieurs tentatives.")

def main():
    # Définir l'ID de la table BigQuery et le chemin des credentials
    table_id = "your_project.your_dataset.your_table"
    credentials_path = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')

    extractor = UniteLegaleExtractor(table_id, credentials_path)
    last_update_date = extractor.get_last_update_date()
    extractor.get_and_send_uniteLegale(last_update_date=last_update_date)

    logger.info("=== Script terminé ===")

if __name__ == "__main__":
    main()