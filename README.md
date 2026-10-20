# Pipeline ETL — Impatto dell'AI sugli studenti

Pipeline ETL orchestrata con **Prefect** che porta 50.000 record da Kaggle a un data warehouse su **MongoDB Atlas**, passando per trasformazioni, feature derivate e l'integrazione di una seconda sorgente. A valle, una dashboard **Power BI** legge direttamente dal warehouse.

Progetto finale dell'unità formativa *Data Management* — ITS Academy ICT Piemonte, percorso Data Analyst & AI Specialist (luglio 2026). Lavoro di gruppo (4 persone): mi sono occupato di progettazione e sviluppo della pipeline.

![Architettura](docs/img/architettura.png)

## Cosa fa

1. **Extract** — scarica il dataset [AI Impact on Students](https://www.kaggle.com/datasets/laveshjadon/ai-impact-on-students) (50.000 record, 16 attributi) direttamente da Kaggle.
2. **Transform**
   - binning del GPA iniziale nelle fasce dell'academic standing USA (Probation, Good Standing, Honors, High Honors)
   - binning delle ore settimanali di AI generativa (Low, Medium, High)
   - codifica ordinale delle variabili categoriche (anno di corso, competenza nei prompt, rischio burnout)
   - feature derivate: `GPA_Change` (post − pre semestre) e `Total_Study_Hours`
   - join 1:1 su `Student_ID` con la tabella `sports`, che sta su un cluster MongoDB di staging separato
   - rimozione delle colonne non usate a valle
3. **Load** — upsert incrementale sulla collezione `students_enriched` del cluster warehouse.

## Scelte tecniche

- **Staging e warehouse separati:** due cluster distinti, così un errore nelle trasformazioni non tocca i dati di origine.
- **Retry solo dove serve:** i task che dipendono dalla rete (estrazione da Kaggle, lettura da MongoDB) riprovano automaticamente; le trasformazioni no, perché sono deterministiche e fallirebbero allo stesso modo.
- **Nessun retry sul caricamento:** una scrittura andata a buon fine a metà non si ripete alla cieca.
- **Upsert idempotente:** rilanciare la pipeline non duplica i record. Con dati identici, una seconda esecuzione trova i documenti già presenti e non ne modifica nessuno: i log di caricamento distinguono nuovi, trovati e modificati.
- **Scrittura a blocchi da 1.000 record + indice unico su `Student_ID`:** un unico bulk write da 50.000 operazioni supera i limiti di tempo di un cluster Atlas gratuito, e senza indice ogni upsert scansionerebbe l'intera collezione.
- **Join controllato:** il tipo della chiave è forzato a `int64` su entrambi i lati, perché al ritorno da MongoDB può cambiare e far fallire il join senza errori visibili. Il task verifica anche che il numero di record non cambi.
- **Log e fallimenti tracciati:** ogni task registra l'errore e lo rilancia, così Prefect segna correttamente il task come fallito.

## Automazione

La pipeline è registrata come deployment Prefect su un work pool, con esecuzione giornaliera a mezzanotte (`cron: 0 0 * * *`).

![Esecuzione su Prefect](docs/img/prefect_run.png)

<img src="docs/img/prefect_tasks.png" alt="Sequenza dei task" width="220">

## Dashboard Power BI

La dashboard è collegata alla collezione `students_enriched` del warehouse e legge il dato già trasformato dalla pipeline: le fasce di GPA e di ore di AI, gli attributi derivati e i dati sportivi diventano le dimensioni lungo cui navigare. Copre quattro aree: composizione del campione, uso dell'AI e monte ore di studio, rendimento accademico, benessere (ansia, burnout, dipendenza percepita).

Le grandezze ordinate (fasce di rendimento, intensità d'uso dell'AI) usano una scala di colore sequenziale; le categorie senza ordine usano colori distinti, per non suggerire gerarchie che non esistono.

![Dashboard Power BI](docs/img/dashboard.png)

## Struttura

```
pipeline_AI_students.py                  # la pipeline (flow + task Prefect)
notebooks/eda_ai_impact_students.ipynb   # analisi esplorativa che ha guidato le trasformazioni
dashboard/dashboard_ai_students.pbix     # dashboard Power BI
requirements.txt
.env.example                             # modello per le stringhe di connessione
```

## Come eseguirla

Servono Python 3.11+, due cluster MongoDB Atlas (staging con la collezione `sports`, warehouse vuoto) e credenziali Kaggle configurate per `kagglehub`.

```bash
python -m venv venv
venv\Scripts\activate            # su Windows
pip install -r requirements.txt

copy .env.example .env           # poi inserisci le tue stringhe di connessione
```

**Esecuzione singola, in locale:**

```bash
python pipeline_AI_students.py
```

**Con deployment e work pool** (tre terminali con il venv attivo):

```bash
prefect server start                                   # 1. server e dashboard di monitoraggio
prefect worker start --pool pipeline-pool              # 2. worker che esegue i run
python pipeline_AI_students.py deploy                  # 3. registra il deployment (una volta)
prefect deployment run "pipeline_AI_students/pipeline-ai-students-deployment"
```

## Nota sui dati

Il dataset Kaggle è sintetico, e anche la tabella `sports` è stata generata per il progetto, per dimostrare l'integrazione di due sorgenti. I risultati dell'analisi vanno letti come esercizio di modellazione, non come evidenza empirica.

## Stack

Python · Prefect · pandas · MongoDB Atlas (pymongo) · kagglehub · Power BI
