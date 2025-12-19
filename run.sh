#!/bin/bash

echo "=== DÉMARRAGE DU PIPELINE BDMA ==="

# 1. Créer le dossier HDFS du projet
echo ">>> Création du dossier HDFS /user/root/taxi ..."
hdfs dfs -mkdir -p /user/root/taxi

# 2. Envoyer le dataset brut dans HDFS
echo ">>> Upload du fichier yellow_tripdata_2015-01.csv dans HDFS ..."
hdfs dfs -put -f /root/yellow_tripdata_2015-01.csv /user/root/taxi/

# Vérification
echo ">>> Vérification du contenu de /user/root/taxi"
hdfs dfs -ls /user/root/taxi

# 3. Lancer le script Spark
echo ">>> Exécution du script Spark nyc_taxi_spark_project.py ..."
spark-submit /root/nyc_taxi_spark_project.py

# 4. Vérifier si la sortie nettoyée existe dans HDFS
echo ">>> Vérification de la sortie clean_taxi_singlefile ..."
hdfs dfs -ls /root/clean_taxi_singlefile

echo "=== PIPELINE TERMINÉ AVEC SUCCÈS ==="
