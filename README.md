# rag-multiagent-assurance-vie-etl
Assistant multi-agents RAG pour le traitement de demandes SAV en assurance-vie. Pipeline ETL documentaire, recherche sémantique, réponses sourcées, évaluation de la fiabilité et mécanismes de fallback.


Mon projet
rag-multiagent-assurance-vie-etl/
│
├── README.md
├── requirements.txt
├── .gitignore
├── .env.example
│
├── data/
│   ├── raw/
│   │   └── procedure_rachat_total.txt
│   ├── bronze/
│   │   └── raw_document_sections.parquet
│   ├── silver/
│   │   └── cleaned_document_sections.parquet
│   ├── gold/
│   │   └── rag_document_chunks.parquet
│   └── chroma_db/                    # ignoré par Git
│
├── notebooks/
│   └── etl_procedure_assurance_vie_pyspark.ipynb
│
├── src/
│   ├── config.py
│   │
│   ├── etl/
│   │   ├── __init__.py
│   │   ├── extract.py                 # lecture des PDF, Word ou TXT
│   │   ├── transform.py               # nettoyage, filtrage, normalisation
│   │   ├── security.py                # masquage / exclusion données sensibles
│   │   ├── chunking.py                # découpage du contenu en chunks
│   │   ├── quality_checks.py          # contrôles qualité
│   │   └── pipeline.py                # orchestration Bronze → Silver → Gold
│   │
│   ├── rag/
│   │   ├── __init__.py
│   │   ├── embeddings.py              # sentence-transformers
│   │   ├── vector_store.py            # ChromaDB
│   │   ├── ingestion.py               # indexation chunks + métadonnées
│   │   ├── retrieval.py               # recherche sémantique
│   │   └── prompts.py                 # prompts centralisés
│   │
│   ├── agents/
│   │   ├── __init__.py
│   │   ├── state.py                   # état partagé entre agents
│   │   ├── intent_agent.py            # compréhension de la demande
│   │   ├── retrieval_agent.py         # recherche des sources
│   │   ├── validation_agent.py        # contrôle pertinence/confiance
│   │   ├── generation_agent.py        # réponse finale sourcée
│   │   ├── escalation_agent.py        # fallback vers conseiller
│   │   └── graph.py                   # orchestration LangGraph
│   │
│   ├── evaluation/
│   │   ├── __init__.py
│   │   ├── test_dataset.json
│   │   ├── metrics.py                 # précision retrieval, faithfulness, etc.
│   │   └── evaluate.py
│   │
│   └── utils/
│       ├── __init__.py
│       ├── logging.py
│       └── exceptions.py
│
├── app/
│   └── streamlit_app.py               # interface utilisateur
│
├── tests/
│   ├── test_etl.py
│   ├── test_security.py
│   ├── test_chunking.py
│   ├── test_retrieval.py
│   └── test_agents.py
│
└── docs/
    ├── architecture.md
    ├── data_governance.md
    └── demo_questions.md