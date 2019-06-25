'''
Connect to SQLite database and add a table for a patient's VCF ran through
parse_gatk_variantstotable.py and placed in ~/Downloads/[date]_gene_pheno_source.csv
'''

import sqlite3

patient_vcftable = open('../../Downloads/parsed_vep_haplotypecalled_T550.vcftable', 'r')
patient_number = 'Patient_T550'

# connect to genome_db.db
genomes_db = './genomes_db.db'
connexion = sqlite3.connect(genomes_db)
cursor = connexion.cursor()

# if a table with the same patient's name already exists, drop it
try:
    cursor.execute('DROP TABLE ' + patient_number)
except sqlite3.OperationalError:
    pass

header_line = "CHROM,POS,ID,REF,ALT,QUAL,FILTER,AC,AF,DP,FS,Allele,Consequence,IMPACT,SYMBOL,Gene,Feature_type,Feature,BIOTYPE,EXON,INTRON,HGVSc,HGVSp,cDNA_position,CDS_position,Protein_position,Amino_acids,Codons,Existing_variation,DISTANCE,STRAND,FLAGS,SYMBOL_SOURCE,HGNC_ID,CCDS,SIFT,PolyPhen,HGVS_OFFSET,MAX_AF,MAX_AF_POPS,CLIN_SIG,SOMATIC,PHENO,PUBMED,T550_GT,T550AD,T550DP,T550GQ,T550GT,T550PL"
values_format = ''
for i in range(len(header_line.split(','))):
    values_format += '?,'

# make a new patient table
cursor.execute('CREATE TABLE ' + patient_number + ' (' +
               header_line.split(',')[0] + ' TEXT, ' +
               header_line.split(',')[1] + ' INT, ' +
               header_line.split(',')[2] + ' TEXT, ' +
               header_line.split(',')[3] + ' TEXT, ' +
               header_line.split(',')[4] + ' TEXT, ' +
               header_line.split(',')[5] + ' FLOAT, ' +
               header_line.split(',')[6] + ' TEXT, ' +
               header_line.split(',')[7] + ' INT, ' +
               header_line.split(',')[8] + ' FLOAT, ' +
               header_line.split(',')[9] + ' INT, ' +
               header_line.split(',')[10] + ' FLOAT, ' +
               header_line.split(',')[11] + ' TEXT, ' +
               header_line.split(',')[12] + ' TEXT, ' +
               header_line.split(',')[13] + ' TEXT, ' +
               header_line.split(',')[14] + ' TEXT, ' +
               header_line.split(',')[15] + ' TEXT, ' +
               header_line.split(',')[16] + ' TEXT, ' +
               header_line.split(',')[17] + ' TEXT, ' +
               header_line.split(',')[18] + ' TEXT, ' +
               header_line.split(',')[19] + ' TEXT, ' +
               header_line.split(',')[20] + ' TEXT, ' +
               header_line.split(',')[21] + ' TEXT, ' +
               header_line.split(',')[22] + ' TEXT, ' +
               header_line.split(',')[23] + ' TEXT, ' +
               header_line.split(',')[24] + ' TEXT, ' +
               header_line.split(',')[25] + ' TEXT, ' +
               header_line.split(',')[26] + ' TEXT, ' +
               header_line.split(',')[27] + ' INT, ' +
               header_line.split(',')[28] + ' INT, ' +
               header_line.split(',')[29] + ' TEXT, ' +
               header_line.split(',')[30] + ' TEXT, ' +
               header_line.split(',')[31] + ' INT, ' +
               header_line.split(',')[32] + ' TEXT, ' +
               header_line.split(',')[33] + ' TEXT, ' +
               header_line.split(',')[34] + ' TEXT, ' +
               header_line.split(',')[35] + ' TEXT, ' +
               header_line.split(',')[36] + ' TEXT, ' +
               header_line.split(',')[37] + ' TEXT, ' +
               header_line.split(',')[38] + ' TEXT, ' +
               header_line.split(',')[39] + ' TEXT, ' +
               header_line.split(',')[40] + ' TEXT, ' +
               header_line.split(',')[41] + ' TEXT, ' +
               header_line.split(',')[42] + ' TEXT, ' +
               header_line.split(',')[43] + ' TEXT, ' +
               header_line.split(',')[44] + ' TEXT, ' +
               header_line.split(',')[45] + ' INT, ' +
               header_line.split(',')[46] + ' INT, ' +
               header_line.split(',')[47] + ' TEXT, ' +
               header_line.split(',')[48] + ' TEXT, ' +
               header_line.split(',')[49] + ' TEXT)')

for variant_line in patient_vcftable:
    if variant_line.startswith('chr'):
        # build a tuple for each variant line formatted as (first_col_data, sec_col_data, (...) last_col_data)
        tuple_for_db = variant_line.split('\t')
        # get rid of last "\n"
        tuple_for_db[-1] = tuple_for_db[-1][:-1]
        # check if genotype was called by haplotype caller (last 5 fields) and if not, add empty fields up to header length
        if len(header_line.split(',')) != len(tuple_for_db):
            print('header_line is : {} but variant line is : {} at position {}'.format(len(header_line.split(',')), len(tuple_for_db), tuple_for_db[1]))
            for i in range(len(header_line.split(',')) - len(tuple_for_db)):
                tuple_for_db.append('')

        # insert it into patient table
        cursor.execute('INSERT INTO ' + patient_number + ' (' + header_line + ') VALUES (' + values_format[:-1] + ')', tuple_for_db)
        connexion.commit()
    else:
        pass

cursor.close()
connexion.close()
