from pymongo import InsertOne
from pymongo.errors import BulkWriteError

def insert_records(df=None, db=None, collectionname=None, batch_size=1000):
    
    #Connexion à la collection MongoDB
    collection = db[collectionname]
    
    # Transforme le DataFrame en liste de dictionnaires
    records = df.to_dict(orient="records")
    # Intialise les compteurs
    total_inserted = 0
    total_duplicates = 0

    # Traitement par batchs
    for i in range(0, len(records), batch_size):
        chunk = records[i:i+batch_size]
        # Crée une liste d'opérations InsertOne requise pour bulk_write
        ops = [InsertOne(doc) for doc in chunk]

        try:
            # Ecrit un batch de documents dans la collection MongoDB
            result = collection.bulk_write(ops, ordered=False)
            # Ajoute le nombre d'insertions réussies
            total_inserted += result.inserted_count
            # Si des erreurs sont remontées par bulk_write (doublons)
        except BulkWriteError as bwe:
            # Recupère les erreurs d'écriture
            write_errors = bwe.details.get("writeErrors", [])
            # Compte uniquement les doublons ignorés
            duplicates_count = sum(1 for e in write_errors if e["code"] == 11000)
            # Ajoute le nombre de documents en double ignorés
            total_duplicates += duplicates_count
            # Ajoute le nombre d'insertions réussies - les documents en double
            total_inserted += len(chunk) - duplicates_count
            # Si une exception est rencontrée lors de l'insertion du batch
        except Exception as e:
            print(f"Erreur inattendue lors de l'insertion d'un batch ({len(chunk)} docs) : {e}")

    return total_inserted, total_duplicates