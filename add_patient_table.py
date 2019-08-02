'''
Connect to SQLite database and add a table for a patient's VCF
placed in ~/Desktop/[Patient VCF].vcf
'''

import vcf #import PyVCF
import sqlalchemy

# Connect to genomes_db.db
engine = sqlalchemy.create_engine('sqlite:///../genomes_db.db')

# Get patient's identifier
patient_id = 'Patient_T550'

# If a table with this patient's name already exists, drop it
engine.execute('DROP TABLE IF EXISTS ' + patient_id)
metadata = sqlalchemy.MetaData()
patient_table = sqlalchemy.Table(patient_id, metadata,
                                 sqlalchemy.Column('CHROM', sqlalchemy.String),
                                 sqlalchemy.Column('POS', sqlalchemy.Integer),
                                 sqlalchemy.Column('ID', sqlalchemy.String),
                                 sqlalchemy.Column('REF', sqlalchemy.String),
                                 sqlalchemy.Column('ALT', sqlalchemy.String),
                                 sqlalchemy.Column('QUAL', sqlalchemy.Integer),
                                 sqlalchemy.Column('FILTER', sqlalchemy.String),
                                 sqlalchemy.Column('ENSG', sqlalchemy.String),
                                 )
metadata.create_all(engine)


def get_ensembl_gene_id(record_INFO_gene_list):
    for Ensembl_gene_id in record_INFO_gene_list:
        if Ensembl_gene_id.startswith('ENSG'):
            return Ensembl_gene_id


# Acquire a connection to genomes_db.db and insert data into the patient's table
connection = engine.connect()
patient_vcf_reader = vcf.Reader(open('../../Desktop/HH37JDSXX-4-IDUDI0074_snp_Annotated.vcf', 'r'))
for record in patient_vcf_reader:
    i = 0
    for allele in record.ALT:
        connection.execute(patient_table.insert(),
                           CHROM=record.CHROM,
                           POS=record.POS,
                           ID=record.ID,
                           REF=record.REF,
                           ALT=str(record.ALT[i]),
                           QUAL=record.QUAL,
                           FILTER=str(record.FILTER),
                           ENSG=get_ensembl_gene_id(record.INFO['GI'])
                           )
        i += 1

# Index specified column
sqlalchemy.Index('gene_index', patient_table.c.ENSG)
connection.close()
