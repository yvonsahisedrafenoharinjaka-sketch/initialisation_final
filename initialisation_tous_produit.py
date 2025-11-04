import pyodbc
import pandas as pd
import math
import re


codique = input("🔹 Entrez le codique du bureau à récupérer (ex: 10101) : ").strip()

driver = '{ODBC Driver 17 for SQL Server}'
uid = 'sa'
pwd = '123P@om@456'

conn_cep = pyodbc.connect(
    f'DRIVER={driver};SERVER=154.126.56.88,1700;DATABASE=CEP;UID={uid};PWD={pwd};Encrypt=no;'
)
conn_baseta = pyodbc.connect(
    f'DRIVER={driver};SERVER=154.126.56.88,1700;DATABASE=BaseTA;UID={uid};PWD={pwd};Encrypt=no;'
)
conn_dbtf = pyodbc.connect(
    f'DRIVER={driver};SERVER=154.126.56.88,1600;DATABASE=DB_TF;UID={uid};PWD={pwd};Encrypt=no;'
)

cursor_cep = conn_cep.cursor()
cursor_baseta = conn_baseta.cursor()

print("\n⚙️ Exécution des mises à jour automatiques...\n")

update_to_true_cep = """
UPDATE dbo.TblLivret
SET CompteJoint = 1
WHERE 
    LEFT(NLivret, 5) = ? AND
    (
        (NomCJoint IS NOT NULL AND LTRIM(RTRIM(NomCJoint)) <> '') OR
        (PrenomsCJoint IS NOT NULL AND LTRIM(RTRIM(PrenomsCJoint)) <> '') OR
        (CINCjoint IS NOT NULL AND LTRIM(RTRIM(CINCjoint)) <> '') OR
        (AdrCjoint IS NOT NULL AND LTRIM(RTRIM(AdrCjoint)) <> '')
    )
"""
cursor_cep.execute(update_to_true_cep, codique)
conn_cep.commit()

update_to_false_cep = """
UPDATE dbo.TblLivret
SET CompteJoint = 0, DateCINCJoint = NULL
WHERE 
    LEFT(NLivret, 5) = ? AND
    (
        (NomCJoint IS NULL OR LTRIM(RTRIM(NomCJoint)) = '') AND
        (PrenomsCJoint IS NULL OR LTRIM(RTRIM(PrenomsCJoint)) = '') AND
        (CINCjoint IS NULL OR LTRIM(RTRIM(CINCjoint)) = '') AND
        (AdrCjoint IS NULL OR LTRIM(RTRIM(AdrCjoint)) = '')
    )
"""
cursor_cep.execute(update_to_false_cep, codique)
conn_cep.commit()
print(f"🟢 CEP mis à jour (CompteJoint)")

update_to_true_ta = """
UPDATE dbo.TblCompte
SET CJoint = 1
WHERE 
    LEFT(NCompte, 5) = ? AND
    (
        (NomConj IS NOT NULL AND LTRIM(RTRIM(NomConj)) <> '') OR
        (PrenomConj IS NOT NULL AND LTRIM(RTRIM(PrenomConj)) <> '') OR
        (CINConj IS NOT NULL AND LTRIM(RTRIM(CINConj)) <> '') OR
        (AdressConj IS NOT NULL AND LTRIM(RTRIM(AdressConj)) <> '')
    )
"""
cursor_baseta.execute(update_to_true_ta, codique)
conn_baseta.commit()

update_to_false_ta = """
UPDATE dbo.TblCompte
SET CJoint = 0
WHERE 
    LEFT(NCompte, 5) = ? AND
    (
        (NomConj IS NULL OR LTRIM(RTRIM(NomConj)) = '') AND
        (PrenomConj IS NULL OR LTRIM(RTRIM(PrenomConj)) = '') AND
        (CINConj IS NULL OR LTRIM(RTRIM(CINConj)) = '') AND
        (AdressConj IS NULL OR LTRIM(RTRIM(AdressConj)) = '')
    )
"""
cursor_baseta.execute(update_to_false_ta, codique)
conn_baseta.commit()
print(f"🟢 BaseTA mise à jour (CJoint)\n")

def nettoyer_champs(df):
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].astype(str).apply(lambda x: re.sub(r'[\r\n]+', ' ', x).strip())
        df[col] = df[col].replace({'': None, ' ': None, '<NA>': None})
    return df

def harmoniser_dataframe(df, colonnes_source, slug):
    colonnes_cibles = [
        'n_livret', 'nom_cli', 'prenoms_cli', 'date_naiss_cli', 'lieu_naiss_cli',
        'adresse_cli', 'code_postale', 'cin_cli', 'date_cin', 'code_prof', 'nationalite',
        'nom_pere_cli', 'nom_mere_cli', 'autres', 'date_ouverture', 'code_agent',
        'code_chef_etbs', 'nom_mandataire', 'prenoms_mandataire', 'domicile_mandataire',
        'qualite_mandataire', 'siege', 'solde', 'date_solde', 'pv', 'code_agence',
        'nom_agence', 'nom_prenom_demandeur', 'adr_demandeur', 'cin_demandeur',
        'date_cin_demandeur', 'qlte_demandeur', 'type_client', 'nom_transfert', 'cloture',
        'capitaliser', 'nom_c_joint', 'prenoms_c_joint', 'cin_c_joint', 'date_cin_c_joint',
        'adr_c_joint', 'compte_joint', 'conv_solde', 'conv_pv', 'solde_prec', 'num_tel', 'kodik'
    ]
    df.rename(columns=colonnes_source, inplace=True)
    for col in colonnes_cibles:
        if col not in df.columns:
            df[col] = None
    df['slug'] = slug
    return df[colonnes_cibles + ['slug']]

def exporter_par_blocs(df, codique):
    nb_lignes = len(df)
    if nb_lignes == 0:
        print("⚠️ Aucune donnée à exporter.")
        return
    nb_fichiers = math.ceil(nb_lignes / 10000)
    for i in range(nb_fichiers):
        start = i * 10000
        end = min(start + 10000, nb_lignes)
        bloc = df.iloc[start:end]
        suffix = "dernierpart" if i == nb_fichiers - 1 else f"part{i+1}"
        fichier = f"initialisation_{codique}_{suffix}.csv"
        bloc.to_csv(fichier, index=False, encoding='utf-8-sig', lineterminator='\n', na_rep='')
        print(f"✅ Export {i+1}/{nb_fichiers} → {fichier} ({len(bloc)} lignes)")
    print(f"📦 Export finalisé ({nb_lignes} lignes)\n")

colonnes_cep = {
    'NLivret': 'n_livret', 'NomCLI': 'nom_cli', 'PrénomsCLI': 'prenoms_cli',
    'DatenaissCLI': 'date_naiss_cli', 'LieunaissCLI': 'lieu_naiss_cli', 'AdresseCLI': 'adresse_cli',
    'CodePostale': 'code_postale', 'CINCli': 'cin_cli', 'DateCIN': 'date_cin', 'CodeProf': 'code_prof',
    'Nationalité': 'nationalite', 'NomPèreCLI': 'nom_pere_cli', 'NomMèreCLI': 'nom_mere_cli',
    'Autres': 'autres', 'DateOuverture': 'date_ouverture', 'CodeAgent': 'code_agent',
    'CodeChefEtbs': 'code_chef_etbs', 'NomMandataire': 'nom_mandataire', 'PrénomsMandataire': 'prenoms_mandataire',
    'DomicileMandataire': 'domicile_mandataire', 'QualitéMandataire': 'qualite_mandataire', 'Siège': 'siege',
    'Solde': 'solde', 'DateSolde': 'date_solde', 'PV': 'pv', 'CodeAgence': 'code_agence',
    'NomAgence': 'nom_agence', 'NomPrenomDemandeur': 'nom_prenom_demandeur', 'AdrDemandeur': 'adr_demandeur',
    'CINDemandeur': 'cin_demandeur', 'DateCINDemandeur': 'date_cin_demandeur', 'QlteDemandeur': 'qlte_demandeur',
    'TypeClient': 'type_client', 'NomTransfert': 'nom_transfert', 'Cloture': 'cloture', 'Capitaliser': 'capitaliser',
    'NomCJoint': 'nom_c_joint', 'PrenomsCJoint': 'prenoms_c_joint', 'CINCjoint': 'cin_c_joint',
    'DateCINCJoint': 'date_cin_c_joint', 'AdrCjoint': 'adr_c_joint', 'CompteJoint': 'compte_joint',
    'ConvSolde': 'conv_solde', 'ConvPV': 'conv_pv', 'SoldePrec': 'solde_prec', 'NumTel': 'num_tel', 'kodik': 'kodik'
}

colonnes_baseta = {
    'NCompte': 'n_livret', 'Nom': 'nom_cli', 'Prenoms': 'prenoms_cli', 'DateNaiss': 'date_naiss_cli',
    'LieuNaiss': 'lieu_naiss_cli', 'Adresse': 'adresse_cli', 'CIN': 'cin_cli', 'DateCIN': 'date_cin',
    'Nationalite': 'nationalite', 'Pere': 'nom_pere_cli', 'Mere': 'nom_mere_cli',
    'NomConj': 'nom_c_joint', 'PrenomConj': 'prenoms_c_joint', 'CINConj': 'cin_c_joint',
    'DateCINConj': 'date_cin_c_joint', 'AdressConj': 'adr_c_joint', 'Solde': 'solde',
    'SoldePrec': 'solde_prec', 'QlteDdeur': 'qlte_demandeur',
    'NomPrenomDemandeur': 'nom_prenom_demandeur',
    'AdresseDdeur': 'adr_demandeur', 'CINDdeur': 'cin_demandeur', 'DateCINDdeur': 'date_cin_demandeur',
    'CJoint': 'compte_joint'
}

colonnes_dbtf = {
    'NCompte': 'n_livret', 'Nom': 'nom_cli', 'Prenom': 'prenoms_cli', 'DateNaiss': 'date_naiss_cli',
    'LieuNaiss': 'lieu_naiss_cli', 'Adresse': 'adresse_cli', 'CIN': 'cin_cli', 'DateCIN': 'date_cin',
    'Profession': 'code_prof', 'Nationalite': 'nationalite', 'Telephone': 'num_tel',
    'DateOuverture': 'date_ouverture'
}

df_cep = pd.read_sql("SELECT * FROM dbo.TblLivret WHERE LEFT(NLivret, 5) = ?", conn_cep, params=[codique])
df_baseta = pd.read_sql("SELECT * FROM dbo.TblCompte WHERE LEFT(NCompte, 5) = ?", conn_baseta, params=[codique])
df_dbtf = pd.read_sql("SELECT * FROM dbo.TblComptes WHERE LEFT(NCompte, 5) = ?", conn_dbtf, params=[codique])

df_cep = harmoniser_dataframe(nettoyer_champs(df_cep), colonnes_cep, 'TL')
df_baseta = harmoniser_dataframe(nettoyer_champs(df_baseta), colonnes_baseta, 'TA')
df_dbtf = harmoniser_dataframe(nettoyer_champs(df_dbtf), colonnes_dbtf, 'TF')

df_final = pd.concat([df_cep, df_baseta, df_dbtf], ignore_index=True)

exporter_par_blocs(df_final, codique)

conn_cep.close()
conn_baseta.close()
conn_dbtf.close()
print("\n✅ Export global TL+TA+TF terminé avec succès !")
