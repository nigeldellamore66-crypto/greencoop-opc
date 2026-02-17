FROM python:3.13-slim

# Définir le répertoire de travail
WORKDIR /app

# Installer les dépendances système nécessaires
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copier le fichier requirements s'il existe
COPY requirements.txt* ./

# Installer les dépendances Python
RUN pip install --no-cache-dir -r requirements.txt; 

# Copier le code source
COPY . .

# Créer les dossiers nécessaires
RUN mkdir -p /app/data /app/logs

# Commande par défaut (à adapter selon ton script principal)
CMD ["python", "main.py"]