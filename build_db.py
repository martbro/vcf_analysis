"""
Build SQLite database with one table for phenotype-annotated genes extracted
using mart export from Ensembl website and placed in ~/Downloads/[date]_gene_pheno_source.csv
Drops the old gene_pheno table and makes a new one
"""

import sqlite3

# connect to genome_db.db
genomes_db = "../genomes_db.db"
connexion = sqlite3.connect(genomes_db)
cursor = connexion.cursor()
'''
# if an old gene_pheno table exists, drop it
try:
    cursor.execute("DROP TABLE gene_pheno")
except sqlite3.OperationalError:
    pass

# make a new gene_pheno table
cursor.execute("CREATE TABLE gene_pheno (gene TEXT, pheno TEXT, source TEXT)")

pheno_csv = open("../../Downloads/190410_gene_pheno_source.csv", "r")
for gene_line in pheno_csv:
    if gene_line.startswith("E"):
        # build a tuple from each gene line from Ensembl file formatted as (gene, pheno, source)
        tuple_for_db = gene_line.split(",")
        # get rid of last "\n"
        tuple_for_db[2] = tuple_for_db[2][:-1]
        # insert it into gene_pheno table
        cursor.execute("INSERT INTO gene_pheno (gene, pheno, source) VALUES (?,?,?)", tuple_for_db)
        connexion.commit()
    else:
        pass

cursor.execute("CREATE INDEX pheno_index ON gene_pheno(pheno)")
'''
cursor.execute("CREATE INDEX gene_pheno_gene_index ON gene_pheno(gene)")

cursor.close()
connexion.close()
