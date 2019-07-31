"""
Build SQLite database with one table for phenotype-annotated genes extracted using
mart export from Ensembl website and placed in ~/Downloads/[date]_gene_pheno_source.csv
Drops the old gene_pheno table and makes a new one
"""
import sqlalchemy

# connect to genome_db.db
# The engine object is a repository for database connections capable of issuing SQL to the database
engine = sqlalchemy.create_engine("sqlite:///../genomes_db.db")

# if an old gene_pheno table exists, drop it
engine.execute("DROP TABLE IF EXISTS gene_pheno")

# make a new gene_pheno table and index specified columns
metadata = sqlalchemy.MetaData()
gene_pheno = sqlalchemy.Table('gene_pheno', metadata,
                              sqlalchemy.Column('gene', sqlalchemy.String),
                              sqlalchemy.Column('pheno', sqlalchemy.String),
                              sqlalchemy.Column('source', sqlalchemy.String),
                              )
sqlalchemy.Index("gene_pheno_gene_index", gene_pheno.columns.gene)

# Tell the MetaData we’d actually like to create our selection of tables for real inside the SQLite database
metadata.create_all(engine)

# Acquire a connection to genomes_db.db and insert data into the gene_pheno table
connection = engine.connect()
ins = gene_pheno.insert()
pheno_csv = open("../../Downloads/190410_gene_pheno_source.csv", "r")
for gene_line in pheno_csv:
    if gene_line.startswith("E"):
        # build a tuple from each gene line from Ensembl file formatted as (gene, pheno, source)
        tuple_for_db = gene_line.split(",")
        connection.execute(ins, gene=tuple_for_db[0], pheno=tuple_for_db[1], source=tuple_for_db[2][:-1])
    else:
        pass

connection.close()
