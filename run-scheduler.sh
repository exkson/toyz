#!/bin/bash
# Scheduler pour lancer le scraper toutes les 15 minutes

echo "=========================================="
echo "CryptoViz Scraper Scheduler Started"
echo "Interval: 15 minutes"
echo "=========================================="

# Créer le répertoire de données si nécessaire
mkdir -p /data

# Première exécution immédiate
echo "[$(date)] Starting initial scrape..."
python3 scraper-kafka-wrapper.py

# Boucle infinie : attendre 15 minutes puis relancer
while true; do
    echo "[$(date)] Waiting 15 minutes for next scrape..."
    sleep 900  # 900 secondes = 15 minutes
    
    echo "[$(date)] Starting scheduled scrape..."
    python3 scraper-kafka-wrapper.py
done
