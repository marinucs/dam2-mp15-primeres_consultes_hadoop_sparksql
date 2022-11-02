# Programa que realiza una serie de consultas desde un fichero json mediante el uso de Hadoop Spark
#!/usr/bin/python3.10
# -*- coding: utf-8 -*-

import findspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession


def main():
    # Inicialitzem Spark
    findspark.init()

    # Creating a spark session
    spark = SparkSession \
        .builder \
        .appName("DAM-M15. Python Spark DataFrames: ús bàsic") \
        .config("spark.opció.configuració", "valor de configuració") \
        .getOrCreate()

    df = spark.read.json("movies.json").cache()

    df.createTempView("movies")
    print("Nombre pelicules al fitxer")
    spark.sql("SELECT COUNT(id) AS NOMBRE_PELICULES FROM movies").show()
    print("Mitja de la puntuació de les pel·licules desades al fitxer")
    spark.sql("SELECT AVG(punts) AS MITJA_PUNTUACIO FROM movies").show()
    print("Mitja de la puntuació de les pel·licules que comencen per la lletra 'S'")
    spark.sql("SELECT AVG(punts) AS MITJA_PUNTUACIO_LLETRA_S FROM movies WHERE UPPER(titol) LIKE 'S%'").show()
    print("Mitja dels vots")
    spark.sql("SELECT AVG(vots) AS MITJA_VOTS FROM movies").show()
    print("Total dels vots de totes les pel·licules")
    spark.sql("SELECT SUM(vots) AS SUMA_VOTS FROM movies").show()
    print("Nom de la pel·licula més antiga")
    spark.sql("SELECT any, titol AS PELICULA_MES_ANTIGA FROM movies WHERE any <= (SELECT MIN(any) from movies)").show(n=50, truncate=False)
    print("Nom de les pel·licules més noves")
    spark.sql("SELECT id, any, titol AS PELICULES_MES_NOVES FROM movies WHERE any >= (SELECT MAX(any) from movies)").show(n=100, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
