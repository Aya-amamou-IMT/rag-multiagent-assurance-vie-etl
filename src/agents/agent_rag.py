
import chromadb

client = chromadb.PersistentClient(path="./data/chroma_db")
collection = client.get_or_create_collection(
    name="procedure_assurance_vie"
)

collection.add(
    ids=["AV-RACHAT-014_1"],
    documents=["Après réception d un dossier complet, le délai indicatif est de 10 à 15 jours ouvrés."],
    metadatas=[{
        "section_title": "Délais",
        "product": "assurance-vie",
        "theme": "rachat-total",
        "version": "3.2",
        "source_reference": "procedure://AV-RACHAT-014/delais"
    }]
)

def rechercher_sources(question: str, n_resultats: int = 3):
    return collection.query(
        query_texts=[question],
        n_results=n_resultats
    )

resultats = rechercher_sources(
    "Quels documents faut-il fournir pour le rachat d'un contrat détenu par un mineur ?"
)

print(resultats["documents"])
print(resultats["metadatas"])


def agent_rag(question: str):
    resultats = rechercher_sources(question)

    sources = resultats["documents"][0]
    metadatas = resultats["metadatas"][0]

    contexte = "\n\n".join(sources)

    prompt = f"""
Tu es un assistant pour des conseillers SAV en assurance-vie.

Réponds uniquement à partir du contexte ci-dessous.
Si l'information est absente ou insuffisante, réponds :
"Je ne dispose pas d'une source suffisamment fiable. Veuillez orienter la demande vers un conseiller spécialisé."

Contexte :
{contexte}

Question :
{question}

Donne une réponse claire et cite la section source utilisée.
"""

    # Appel à ton LLM : Azure OpenAI, OpenAI API, ou modèle local
    reponse = appeler_llm(prompt)

    return {
        "question": question,
        "reponse": reponse,
        "sources": metadatas
    }