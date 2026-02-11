from pymongo import MongoClient

# Connexion à la base de donnée
def get_db(uri, dbname):
    client = MongoClient(uri)
    return client[dbname]

# Création utilisateur
def create_user(db_admin,MONGO_USER,MONGO_PWD,DB_NAME):
    try:
        # Vérifie si l'utilisateur existe déjà sur admin
        users = db_admin.command("usersInfo", MONGO_USER)
        if users.get("users"):
            print(f"L'utilisateur `{MONGO_USER}` existe déjà sur admin.")
            return

        # Crée l'utilisateur sur admin avec rôle sur DB_NAME
        db_admin.command(
            "createUser",
            MONGO_USER,
            pwd=MONGO_PWD,
            roles=[{"role": "readWrite", "db": DB_NAME}]
        )
        print(f"Utilisateur `{MONGO_USER}` créé sur admin avec accès à `{DB_NAME}`")

    except Exception as e:
        print("Erreur lors de la création d'utilisateur :")
        print(e)
        raise

# Création index unique sur la collection
def index_unique(db, collectionname):

    collection = db[collectionname]
    collection.create_index(
        [
            ("station_id", 1),
            ("timestamp", 1),
        ],
        unique=True
    )
    print("Index unique appliqué sur (staton_id, timestamp)")