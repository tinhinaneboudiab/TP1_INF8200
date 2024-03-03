#Importation des bibliotheques
import random
import string
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
def generate_spending(spending_categories):
    return [random.randint(100, 1000) for _ in spending_categories]

def generate_codes(n):
    codes = []
    for _ in range(n):
        letters = ''.join(random.choices(string.ascii_uppercase, k=4))
        numbers = ''.join(random.choices(string.digits, k=8))
        code = letters + numbers
        codes.append(code)
    return codes

def fake_source():
    # List of student names
    users = ['BAKA67300004', 'BOUT79360000', 'CONV09089808', 'DIAS03299509', 'DICH19079502',
             'FOFM64270305', 'GBEH24279505', 'JEAE20118602', 'LAFG13039809', 'LOXS25369509',
             'MEDY29339203', 'NDIA68270100', 'NIAK12339405', 'NOME15269503', 'SONJ86350009',
             'SONJ86350009', 'SOWM19289605', 'TOHD13369601']
    users += generate_codes(20)

    spending_categories = ['Compute', 'Storage', 'Networking', 'Database', 'Analytics']
    user_spendings = [(user, *generate_spending(spending_categories)) for user in users]
    return user_spendings, ['userID'] + spending_categories

def read_data_source(spark):
    data, cols = fake_source()
    return spark.createDataFrame(data=data, schema = cols)


if __name__ == "__main__":
    # Création de la session spark
    spark = SparkSession.builder.appName("Tp1").getOrCreate()

    # Appel à la fonction read_data_source
    df = read_data_source(spark)

    # 1. Affichage de 20 premières lignes de DataFrame simulé
    print("Affichage de 20 premieres lignes de DataFrame")
    df.show(truncate=False)

    # 2. L'ajout d'une colonne `Total` contenant le total des dépenses
    result=df.withColumn('Total',expr("Compute+Storage+Networking+Database+Analytics"))

    # 3. Affichage de 20 premières lignes de DataFrame aprés l'Ajout de la colonne total
    result.show()

    # 4. Création de la table `depenses`
    # 4.1. Création de la vue à partir de notre DataFrame
    result.createOrReplaceTempView("depenses")

    # 4.2. Interrogation de la vue à l'aide de requetes SQL.
    df_vue = spark.sql("SELECT * FROM depenses")

    # 4.3. Affichage de 20 premières ligne de la table.
    df_vue.show() 

    # 5. Le nombre d'entrée de table résultante de l'ETL
    Nb_Entries= spark.sql("SELECT COUNT(storage) FROM depenses ")
    Nb_Entries.show()

    # 6. La moyenne de la somme total des depenses
    Mean_depenses=spark.sql("SELECT ROUND(AVG(Total),2) AS Mean_depenses FROM depenses")
    Mean_depenses.show()

    #  liste des depenses incluant la somme de l'utilisateur dont le userID est mon codePermanant
    Mes_depenses=spark.sql("SELECT * FROM depenses where userId='BOUT79360000'")
    Mes_depenses.show()
    
    spark.stop()