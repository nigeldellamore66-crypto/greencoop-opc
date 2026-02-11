from typing import List, Tuple, Dict
from datetime import datetime
import pandas as pd

def calculer_taux_erreur(db, collection_name, dataframes_sources):
# Compare les dataframes sources avec les données présentes dans la collection

    print(f"\nValidation de {collection_name}")
    print("="*50)
    
    # 1. Compter ce qu'on attendait
    total_attendu = sum(len(df) for nom, df in dataframes_sources)
    print(f"Lignes envoyées: {total_attendu}")
    
    # 2. Compter ce qu'on a dans MongoDB
    collection = db[collection_name]
    total_recu = collection.count_documents({})
    print(f"Documents reçus: {total_recu}")
    
    # 3. Calculer l'erreur
    if total_recu == total_attendu:
        print("Migration parfaite !")
        taux_erreur = 0.0
        succes = True
    else:
        perte = abs(total_attendu - total_recu)
        taux_erreur = (perte / total_attendu) * 100
        print(f"Perte: {perte} documents ({taux_erreur:.2f}%)")
        succes = False
    
    # 4. Vérifier par source (optionnel mais utile)
    print("\nDétail par source:")
    for nom, df in dataframes_sources:
        attendu = len(df)
        recu = collection.count_documents({'source': nom})
        
        if recu == attendu:
            print(f"  {nom}: {recu}/{attendu}")
        else:
            print(f"  {nom}: {recu}/{attendu}")
    
    print("="*50)
    
    return {
        'collection': collection_name,
        'attendu': total_attendu,
        'recu': total_recu,
        'taux_erreur': taux_erreur,
        'succes': succes
    }