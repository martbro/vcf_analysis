import sqlite3

# connect to genomes.db database
fichier_donnees = "./genomes_db.db"
connexion = sqlite3.connect(fichier_donnees)
cursor = connexion.cursor()

# ask for which phenotype to search for and query the database with %requested_pheno%
#print("Phenotype recherché :")
#requested_pheno = "%" + "%".join(input().split()) + "%"
cursor.execute("SELECT * FROM Patient_T550 INNER JOIN gene_pheno ON Patient_T550.Gene = gene_pheno.gene WHERE gene_pheno.pheno LIKE '%MERRF%'")
print(cursor.fetchall())

cursor.close()
connexion.close()