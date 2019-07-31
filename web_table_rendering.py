from flask import Flask, request, render_template
import sqlite3

app = Flask(__name__)


@app.route('/')
def my_form():
    return render_template('./my-form.html')


@app.route('/', methods=['POST'])
def my_form_post():
    # connect to genomes.db database
    genomes_db = "../genomes_db.db"
    connexion = sqlite3.connect(genomes_db)
    cursor = connexion.cursor()
    requested_adapted_pheno = "%" + "%".join(request.form['pheno'].split()) + "%"
    requested_adapted_source = "%" + "%".join(request.form['source'].split()) + "%"
    print(request.form)
    cursor.execute("SELECT DISTINCT CHROM, POS, REF, ALT, QUAL, AC, AF, DP, SYMBOL, Patient_T550.Gene, SIFT, PolyPhen, MAX_AF, MAX_AF_POPS, T550GT, gene_pheno.gene, gene_pheno.pheno, source "
                   "FROM Patient_T550 "
                   "INNER JOIN gene_pheno ON Patient_T550.Gene = gene_pheno.gene "
                   "WHERE gene_pheno.source LIKE '" + requested_adapted_source + "' "
                        "AND gene_pheno.pheno LIKE '" + requested_adapted_pheno + "' "
                        "AND Patient_T550.AF>" + request.form['all_fraction'] + " "
                        "AND Patient_T550.MAX_AF<" + request.form['freq_cut_off'] + " "
                    "ORDER BY MAX_AF")
    results = cursor.fetchall()
    cursor.close()
    connexion.close()
    return render_template('./my-form.html', sqlite_query=results)
