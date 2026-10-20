"""
Pipeline Data Management - Impatto AI sugli studenti
====================================================
ETL con Prefect: estrazione da Kaggle, trasformazioni, join con la
tabella sport (MongoDB Atlas raw), caricamento sul data warehouse
(MongoDB Atlas DWH) con upsert incrementale.

Robustezza:
  - retry automatici SOLO sui task che dipendono dalla rete
    (extract da Kaggle, join_sport da Mongo): un errore di rete e'
    transitorio, un ValueError su un binning e' deterministico e
    riprovarlo non cambia l'esito
  - NESSUN retry sul load
  - timeout di connessione Mongo a 60s (serverSelectionTimeoutMS)
  - ogni task logga l'errore (logger.error) e poi lo rilancia (raise),
    cosi' Prefect segna il task come fallito
"""

import os
import certifi
from datetime import datetime
import pandas as pd
import kagglehub
from kagglehub import KaggleDatasetAdapter
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne
from pymongo.errors import PyMongoError
from prefect import task, flow, get_run_logger
from pathlib import Path


# --- CONNESSIONI MONGODB ---
# Le stringhe di connessione si leggono dal file .env (vedi .env.example),
# che resta fuori dal repository. Nessuna credenziale nel codice.
load_dotenv()

MONGO_URI_RAW = os.getenv("MONGO_URI_RAW")   # cluster di staging: da qui arriva la tabella sport
MONGO_URI_DWH = os.getenv("MONGO_URI_DWH")   # cluster warehouse: destinazione del load

if not MONGO_URI_RAW or not MONGO_URI_DWH:
    raise RuntimeError(
        "Variabili MONGO_URI_RAW e MONGO_URI_DWH non impostate: "
        "copia .env.example in .env e inserisci le tue stringhe di connessione."
    )

DB_RAW = "ai_student_impact_sport"      # cluster raw: qui vive la tabella sport
COLL_SPORT = "sports"
DB_DWH = "ai_student_impact_dwh"        # cluster warehouse: destinazione finale
COLL_OUT = "students_enriched"

KAGGLE_SLUG = "laveshjadon/ai-impact-on-students"
KAGGLE_FILE = "ai_student_impact_dataset (1).csv"

CONN_TIMEOUT_MS = 60000                 # 60s: timeout selezione server Mongo


# =============================== EXTRACT =====================================
# Retry SI: dipende dalla rete e dall'API di Kaggle, l'errore puo' essere transitorio.
@task(retries=2, retry_delay_seconds=10)
def extract_from_kaggle():
    """Scarica il dataset base da Kaggle e lo restituisce come DataFrame."""
    logger = get_run_logger()
    try:
        df = kagglehub.load_dataset(
            KaggleDatasetAdapter.PANDAS,
            KAGGLE_SLUG,
            KAGGLE_FILE,
        )
    except Exception as e:
        logger.error(f"[EXTRACT] estrazione da Kaggle fallita: {e}")
        raise

    logger.info(f"[EXTRACT] {len(df)} record, {df.shape[1]} colonne")
    return df


# ========================== TRANSFORM ===========================
# niente retries sui transform. Sono funzioni pure e deterministiche:
# se falliscono al primo tentativo falliranno identiche al secondo e al terzo.
# Il retry serve solo dove l'errore puo' essere transitorio (rete/DB).

@task
def binning_GPA(df):
    """
    Segmenta Pre_Semester_GPA nelle fasce dell'academic standing USA.
    Soglie fisse convenzionali:
      < 2.0   -> Probation
      2.0-3.0 -> Good Standing
      3.0-3.5 -> Honors
      3.5-4.0 -> High Honors
    right=False -> intervalli [lower, upper).
    """
    logger = get_run_logger()
    try:
        df = df.copy()   # lavoro su una copia, non muto l'input del task

        bin_edges = [0.0, 2.0, 3.0, 3.5, 4.01]   # 4.01 include un eventuale 4.0 esatto
        bin_labels = ["Probation", "Good Standing", "Honors", "High Honors"]

        df["Pre_Semester_GPA_Bin"] = pd.cut(
            df["Pre_Semester_GPA"], bins=bin_edges, labels=bin_labels, right=False,
        )

        n_missing = df["Pre_Semester_GPA_Bin"].isna().sum()
        if n_missing > 0:
            raise ValueError(f"{n_missing} valori GPA non assegnati a un bin")
    except Exception as e:
        logger.error(f"[TRANSFORM binning_GPA] errore: {e}")
        raise

    logger.info(f"[TRANSFORM binning_GPA] fasce: {df['Pre_Semester_GPA_Bin'].value_counts().to_dict()}")
    return df


@task
def binning_AI_hours(df):
    """
    Segmenta Weekly_GenAI_Hours in tre fasce di intensita' d'uso:
      < 2h    -> Low     (uso basso)
      2-15h   -> Medium  (uso medio)
      >= 15h  -> High    (uso intensivo)
    right=False -> intervalli [lower, upper).
    """
    logger = get_run_logger()
    try:
        df = df.copy()   

        bin_edges = [0.0, 2.0, 15.0, 100.0]   # 100 come tetto: max reale = 40h
        bin_labels = ["Low", "Medium", "High"]

        df["Weekly_GenAI_Hours_Bin"] = pd.cut(
            df["Weekly_GenAI_Hours"], bins=bin_edges, labels=bin_labels, right=False,
        )

        n_missing = df["Weekly_GenAI_Hours_Bin"].isna().sum()
        if n_missing > 0:
            raise ValueError(f"{n_missing} valori ore AI non assegnati a un bin")
    except Exception as e:
        logger.error(f"[TRANSFORM binning_AI_hours] errore: {e}")
        raise

    logger.info(f"[TRANSFORM binning_AI_hours] fasce: {df['Weekly_GenAI_Hours_Bin'].value_counts().to_dict()}")
    return df


@task
def encode_categoricals(df):
    """Ordina le colonne per avere la sequenzialita' corretta su PowerBI."""
    logger = get_run_logger()
    try:
        df = df.copy()   

        orders = {
            "Year_of_Study": ["Freshman", "Sophomore", "Junior", "Senior", "Graduate"],
            "Prompt_Engineering_Skill": ["Beginner", "Intermediate", "Advanced"],
            "Burnout_Risk_Level": ["Low", "Medium", "High"],
        }
        for col, order in orders.items():
            df[col] = pd.Categorical(df[col], categories=order, ordered=True)
            n_nan = df[col].isna().sum()
            if n_nan > 0:
                raise ValueError(f"{col}: {n_nan} valori non coperti dalle categorie {order}")
    except Exception as e:
        logger.error(f"[TRANSFORM encode_categoricals] errore: {e}")
        raise

    logger.info(f"[TRANSFORM encode_categoricals] convertite: {list(orders.keys())}")
    return df


@task
def add_derived_features(df):
    """
    Attributi derivati:
      - GPA_Change: variazione di GPA (Post - Pre), indicatore di esito finale rendimento
      - Total_Study_Hours: monte ore totale di apprendimento a settimana
        (ore con l'AI + ore di studio tradizionale)
    """
    logger = get_run_logger()
    try:
        df = df.copy()   

        df["GPA_Change"] = df["Post_Semester_GPA"] - df["Pre_Semester_GPA"]
        df["Total_Study_Hours"] = df["Weekly_GenAI_Hours"] + df["Traditional_Study_Hours"]
    except Exception as e:
        logger.error(f"[TRANSFORM add_derived_features] errore: {e}")
        raise

    logger.info(
        f"[TRANSFORM add_derived_features] GPA_Change media {df['GPA_Change'].mean():.3f} | "
        f"Total_Study_Hours media {df['Total_Study_Hours'].mean():.2f}h"
    )
    return df


@task
def drop_columns(df):
    """
    Rimuove colonne non utilizzate a valle (dashboard/analisi):
    Paid_Subscription e Institutional_Policy.
    """
    logger = get_run_logger()
    try:
        to_drop = ["Paid_Subscription", "Institutional_Policy"]
        present = [c for c in to_drop if c in df.columns]
        df = df.drop(columns=present)   # drop() restituisce gia' una copia
    except Exception as e:
        logger.error(f"[TRANSFORM drop_columns] errore: {e}")
        raise

    logger.info(f"[TRANSFORM drop_columns] rimosse: {present} | colonne restanti: {df.shape[1]}")
    return df


# Retry SI: legge da MongoDB, l'errore di connessione puo' essere transitorio.
@task(retries=2, retry_delay_seconds=10)
def join_sport(df):
    """
    Integra la tabella sport (collezione 'sports', cluster RAW su MongoDB).
    Merge 1:1 su Student_ID.
    Il tipo della chiave e' forzato a int64 su entrambi i lati: al ritorno da
    Mongo puo' cambiare e far fallire il join in silenzio.
    """
    logger = get_run_logger()

    n_unmatched = 0   # inizializzata prima del try perche' viene usata anche dopo:
                      # se fosse definita solo dentro, spostare una riga
                      # basterebbe a causare un NameError.
    try:
        df = df.copy()   

        client = MongoClient(MONGO_URI_RAW, tlsCAFile=certifi.where(),
                             serverSelectionTimeoutMS=CONN_TIMEOUT_MS)
        try:
            sports = pd.DataFrame(list(client[DB_RAW][COLL_SPORT].find({}, {"_id": 0})))
        finally:
            client.close()

        if sports.empty:
            raise ValueError("La collezione 'sports' e' vuota o non raggiungibile")

        df["Student_ID"] = df["Student_ID"].astype("int64")
        sports["Student_ID"] = sports["Student_ID"].astype("int64")

        n_before = len(df)
        df = df.merge(sports, on="Student_ID", how="left")

        if len(df) != n_before:
            raise ValueError(f"Il join ha alterato i record: {n_before} -> {len(df)}")

        n_unmatched = df["Sport"].isna().sum()
        if n_unmatched == len(df):
            raise ValueError("Join fallito: nessuna corrispondenza (controlla il tipo di Student_ID)")
    except PyMongoError as e:
        logger.error(f"[TRANSFORM join_sport] errore MongoDB (raw): {e}")
        raise
    except Exception as e:
        logger.error(f"[TRANSFORM join_sport] errore: {e}")
        raise

    if n_unmatched > 0:
        logger.warning(f"[TRANSFORM join_sport] {n_unmatched} studenti senza match sport")
    logger.info(f"[TRANSFORM join_sport] merge ok: {len(df)} record, {df.shape[1]} colonne")
    return df


# ============================= LOAD =============================
# Nessun retry: una scrittura parzialmente andata a buon fine non va ripetuta
# alla cieca. L'upsert e' idempotente, ma il rilancio va deciso a mano.
@task
def load_to_warehouse(df):
    """
    Upsert incrementale su Student_ID nel warehouse (cluster DWH):
    rilanciare il flow non duplica, aggiorna i cambiati, inserisce i nuovi
    (non rimuove quelli gia' esistenti).
    Categorical -> str (pymongo non serializza il tipo Categorical).
    """
    logger = get_run_logger()

    BATCH_SIZE = 1000   # blocchi piccoli: un cluster Atlas free non regge
                        # 50k upsert in un solo bulk_write (timeout Atlas a 10s)

    try:
        df = df.copy()   

        # Categorical -> str: pymongo non sa serializzare il tipo
        for col in df.select_dtypes(include="category").columns:
            df[col] = df[col].astype(str)

        # Mongo ragiona per documenti, non per righe: ogni riga del
        # DataFrame diventa un dizionario {colonna: valore}
        records = df.to_dict(orient="records")
        total = len(records)

        client = MongoClient(MONGO_URI_DWH, tlsCAFile=certifi.where(),
                             serverSelectionTimeoutMS=CONN_TIMEOUT_MS)
        try:
            coll = client[DB_DWH][COLL_OUT]

            # indice unico su Student_ID.
            # Senza indice, ogni UpdateOne fa una collection scan per trovare il
            # documento: con 50k record sono 50k scansioni su un cluster free ->
            # timeout garantito. create_index e' idempotente: se l'indice esiste
            # gia' non fa nulla, quindi si puo' chiamare a ogni run.
            coll.create_index("Student_ID", unique=True)

            upserted = matched = modified = 0   # contatori per il monitoraggio

            # bulk_write
            for start in range(0, total, BATCH_SIZE):
                chunk = records[start:start + BATCH_SIZE]
                # Una UpdateOne per record: filtro su Student_ID, $set dei campi,
                # upsert=True per inserire se il documento non esiste.
                # $set NON rimuove i campi assenti dal record.
                ops = [
                    UpdateOne({"Student_ID": r["Student_ID"]}, {"$set": r}, upsert=True)
                    for r in chunk
                ]
                res = coll.bulk_write(ops, ordered=False)
                upserted += res.upserted_count
                matched += res.matched_count
                modified += res.modified_count   
                logger.info(f"[LOAD] blocco {start + len(chunk)}/{total} scritto")
        finally:
            # chiusura connessione con MongoDB
            client.close()
    except PyMongoError as e:
        # logger per errore lato DB (timeout, autenticazione, connessione)
        logger.error(f"[LOAD] errore MongoDB (warehouse): {e}")
        raise
    except Exception as e:
        # altri errori
        logger.error(f"[LOAD] errore: {e}")
        raise

    # distinzione tra matched e modified.
    #   upserted -> documenti nuovi, inseriti ora
    #   matched  -> documenti gia' presenti che il filtro ha trovato
    #   modified -> di quelli trovati, quanti sono cambiati davvero
    # Prima esecuzione su collezione vuota: nuovi = totale, trovati = 0.
    # Seconda esecuzione con dati identici: nuovi = 0, trovati = totale,
    #   modificati = 0  <- e' questa la prova che l'upsert e' incrementale
    #   e non sta riscrivendo tutto ogni volta.
    logger.info(
        f"[LOAD] upsert su {COLL_OUT} @ {datetime.now().isoformat(timespec='seconds')} | "
        f"nuovi: {upserted}, trovati: {matched}, modificati: {modified}, "
        f"totale elaborati: {total}"
    )
    return total
    # NOTA: $set aggiunge/aggiorna i campi ma NON rimuove quelli assenti dal
    # DataFrame. drop_columns toglie Paid_Subscription e Institutional_Policy,
    # ma se erano gia' stati caricati in una run precedente restano nei documenti
    # come campi "fantasma". Vanno rimossi una volta sola, a mano, da Atlas:
    #   db.students_enriched.updateMany(
    #       {}, {"$unset": {"Paid_Subscription": "", "Institutional_Policy": ""}}
    #   )
    # oppure svuotando la collezione prima del primo rilancio con la drop attiva.


# ============================= FLOW =============================
@flow(name="pipeline_AI_students")
def pipeline():
    logger = get_run_logger()
    start = datetime.now()
    logger.info(f"[PIPELINE] avvio {start.isoformat(timespec='seconds')}")

    df = extract_from_kaggle()
    df = binning_GPA(df)
    df = binning_AI_hours(df)
    df = encode_categoricals(df)
    df = add_derived_features(df)
    df = join_sport(df)
    df = drop_columns(df)          # ultimo transform: toglie le colonne solo prima del load
    n = load_to_warehouse(df)

    logger.info(f"[PIPELINE] completata: {n} record in {(datetime.now()-start).seconds}s")


if __name__ == "__main__":
    # Due modalita':
    #   python pipeline_AI_students.py          -> esegue la pipeline in locale (per testare)
    #   python pipeline_AI_students.py deploy   -> registra il deployment sul work pool
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "deploy":
        pipeline.from_source(
            source=str(Path(__file__).parent),              # scrittura per workpool
            entrypoint="pipeline_AI_students.py:pipeline",
        ).deploy(
            name="pipeline-ai-students-deployment",
            work_pool_name="pipeline-pool",
            cron="0 0 * * *",                               # scheduled time
        )
    else:
        pipeline()
