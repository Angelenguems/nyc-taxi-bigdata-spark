# =====================================================================
# Projet BDMA UA3 – Analyse de Données Massives
# Étudiants :- Angele Blandine Feussi Nguemkam
#            - Traore Fadimatou
#            - Yanis Hamek

# Date : decembre 2025
#
# Dataset : NYC Yellow Taxi Trip Data – January 2015
# Source : https://www.kaggle.com/datasets/elemento/nyc-yellow-taxi-trip-data
#
# Framework utilisé : Apache Spark (DataFrames + RDD)
#
# Étapes implémentées :
#   1. Chargement du CSV depuis HDFS
#   2. Sélection des colonnes utiles
#   3. Vérification de la qualité (valeurs nulles, incohérences)
#   4. Nettoyage du dataset (filtres + suppression des doublons)
#   5. Transformations (≥ 5 différentes) et actions (≥ 3)
#   6. Analyse exploratoire : 3 questions
#   7. Statistiques descriptives
#   8. Sauvegarde finale du dataset nettoyé dans HDFS
#
#  **code source officiel**
# =====================================================================

from pyspark.sql import SparkSession, functions as F


def main():
    # ---------------------------------------------------------
    # 1. Création de la SparkSession
    # ---------------------------------------------------------
    spark = (
        SparkSession.builder
        .appName("BDMA_UA3_NYC_Taxi_Jan2015")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print("\n=== 1. Chargement des données depuis HDFS ===")

    # Chargement du dataset depuis HDFS
    hdfs_path = "hdfs://hadoop-master:9000/user/root/taxi/yellow_tripdata_2015-01.csv"

    df = (
        spark.read
        .csv(hdfs_path, header=True, inferSchema=True)
    )

    # Action 1 : count() pour obtenir le nombre total de lignes
    total_rows = df.count()
    print(f"Nombre total de lignes (brut) : {total_rows}")

    print("\nAperçu des premières lignes (dataset brut) :")
    df.show(5)

    print("\nSchéma du dataset brut :")
    df.printSchema()

    # ---------------------------------------------------------
    # 2. Sélection des colonnes utiles
    # ---------------------------------------------------------
    print("\n=== 2. Sélection des colonnes utiles ===")

    colonnes_utiles = [
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "fare_amount",
        "tip_amount",
        "total_amount",
        "payment_type",
    ]

    df_sel = df.select(colonnes_utiles)
    df_sel.show(5)
    df_sel.printSchema()

    # ---------------------------------------------------------
    # 3. Vérification de la qualité 
    # ---------------------------------------------------------
    print("\n=== 3. Vérification de la qualité ===")

    print("\n3.1 Valeurs nulles :")
    df_sel.select([
        F.count(F.when(F.col(c).isNull(), c)).alias(c)
        for c in df_sel.columns
    ]).show()

    print("\n3.2 Valeurs incohérentes :")
    print("Passagers ≤ 0 :", df_sel.filter(F.col("passenger_count") <= 0).count())
    print("Distance ≤ 0 :", df_sel.filter(F.col("trip_distance") <= 0).count())
    print("fare_amount < 0 :", df_sel.filter(F.col("fare_amount") < 0).count())
    print("total_amount ≤ 0 :", df_sel.filter(F.col("total_amount") <= 0).count())
    print("tip_amount < 0 :", df_sel.filter(F.col("tip_amount") < 0).count())

    # ---------------------------------------------------------
    # 4. Nettoyage
    # ---------------------------------------------------------
    print("\n=== 4. Nettoyage du dataset ===")

    df_clean = df_sel.filter(
        (F.col("passenger_count") > 0) &
        (F.col("trip_distance") > 0) &
        (F.col("fare_amount") >= 0) &
        (F.col("total_amount") > 0) &
        (F.col("tip_amount") >= 0)
    )

    print("\nNombre après nettoyage :", df_clean.count())

    # Suppression des doublons
    df_no_dup_exact = df_clean.dropDuplicates()
    df_final = df_no_dup_exact.dropDuplicates([
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "total_amount"
    ])
    print("\nNombre final après suppression des doublons :", df_final.count())

    df_final.show(5)

    # ---------------------------------------------------------
    # 5. Transformations & actions
    # ---------------------------------------------------------
    print("\n=== 5. Transformations & actions ===")

    # Transformation 1
    df_t1 = df_final.filter(F.col("trip_distance") > 0.5)
    print("Trajets > 0.5 mile :", df_t1.count())

    # Transformation 2
    df_t2 = df_t1.withColumn(
        "duration_min",
        F.round(
            (F.col("tpep_dropoff_datetime").cast("long") -
             F.col("tpep_pickup_datetime").cast("long")) / 60, 2
        )
    )
    df_t2.show(5)

    # Transformation 3
    df_t3 = df_t2.select("trip_distance", "duration_min", "passenger_count")
    df_t3.show(5)

    # Transformation 4
    df_t4 = df_t3.groupBy("passenger_count").count()
    df_t4.show()

    # Transformation 5 (RDD)
    rdd = df_final.rdd.map(
        lambda row: (row.tpep_pickup_datetime.hour, row.trip_distance)
    )
    rdd_sum = rdd.reduceByKey(lambda a, b: a + b)
    print("\nRDD example :", rdd_sum.take(10))

    # ---------------------------------------------------------
    # 6. Analyses
    # ---------------------------------------------------------
    print("\n=== 6. Analyses ===")
    # Question 1 : Quelles sont les heures avec le plus de trajets ?

    df_h = df_final.withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))

    print("\n→ Heures avec le plus de trajets :")
    df_h.groupBy("pickup_hour").count().orderBy("count", False).show(5)
    # Question 2 : Quelle est la distance totale parcourue par heure ?

    print("\n→ Distance totale par heure :")
    df_h.groupBy("pickup_hour").agg(F.sum("trip_distance")).show(5)
    # Question 3 : Quel est le pourboire moyen par type de paiement ?

    print("\n→ Pourboire moyen par type de paiement :")
    df_final.groupBy("payment_type").agg(F.avg("tip_amount")).show()

    # ---------------------------------------------------------
    # 7. Statistiques descriptives
    # ---------------------------------------------------------
    print("\n=== 7. Statistiques descriptives ===")
    df_final.describe().show()

    # ---------------------------------------------------------
    # 8. Sauvegarde
    # ---------------------------------------------------------
    print("\n=== 8. Sauvegarde du dataset nettoyé ===")

    output_hdfs = "/root/clean_taxi_singlefile"

    df_final.coalesce(1).write.mode("overwrite").option("header", True).csv(output_hdfs)

    print(f"Dataset sauvegardé dans : {output_hdfs}")
    print("\n=== Fin du script ===")

    spark.stop()


if __name__ == "__main__":
    main()
