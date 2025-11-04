import pyodbc
import pandas as pd
import math
import re

# === 🔹 Saisie du codique ===
codique = input("🔹 Entrez le codique du bureau à récupérer (ex: 10101) : ").strip()

# === 🔹 Connexion SQL Server ===
driver = '{ODBC Driver 17 for SQL Server}'
conn_cep = pyodbc.connect(
    f'DRIVER={driver};'
    f'SERVER=154.126.56.88,1700;'
    f'DATABASE=CEP;'
    f'UID=sa;'
    f'PWD=123P@om@456;'
    f'Encrypt=no;'
)
cursor = conn_cep.cursor()

print("\n⚙️ Exécution des mises à jour automatiques...\n")

# === 1️⃣ CompteJoint = 1 si conjoint existe ===
update_to_true = """
UPDATE dbo.TblLivret
SET CompteJoint = 1
WHERE 
    LEFT(NLivret, 5) = ? AND
    (
        (NomCJoint IS NOT NULL AND LTRIM(RTRIM(NomCJoint)) <> '') OR
        (PrenomsCJoint IS NOT NULL AND LTRIM(RTRIM(PrenomsCJoint)) <> '') OR
        (CINCjoint IS NOT NULL AND LTRIM(RTRIM(CINCjoint)) <> '') OR
        (AdrCjoint IS NOT NULL AND LTRIM(RTRIM(AdrCjoint)) <> '') OR
        (CompteJoint IS NULL)
    )
"""
cursor.execute(update_to_true, codique)
conn_cep.commit()

# === 2️⃣ CompteJoint = 0 + DateCINCJoint = NULL si tout vide ===
update_to_false = """
UPDATE dbo.TblLivret
SET 
    CompteJoint = 0,
    DateCINCJoint = NULL
WHERE 
    LEFT(NLivret, 5) = ? AND
    CompteJoint = 1 AND
    (
        (NomCJoint IS NULL OR LTRIM(RTRIM(NomCJoint)) = '') AND
        (PrenomsCJoint IS NULL OR LTRIM(RTRIM(PrenomsCJoint)) = '') AND
        (CINCjoint IS NULL OR LTRIM(RTRIM(CINCjoint)) = '')
    )
"""
cursor.execute(update_to_false, codique)
conn_cep.commit()

# === 3️⃣ Si DateCIN est NULL → DateOuverture ===
update_datecin = """
UPDATE dbo.TblLivret
SET DateCIN = DateOuverture
WHERE 
    LEFT(NLivret, 5) = ? AND
    (DateCIN IS NULL)
"""
cursor.execute(update_datecin, codique)
conn_cep.commit()

# === 4️⃣ Si CINDemandeur est NULL ou invalide → '900000900000' ===
update_cindemandeur = """
UPDATE dbo.TblLivret
SET CINDemandeur = '900000900000'
WHERE LEFT(NLivret, 5) = ?
AND (
    CINDemandeur IS NULL
    OR LTRIM(RTRIM(CINDemandeur)) = ''
    OR CINDemandeur LIKE '%[^0-9]%'
    OR LEN(LTRIM(RTRIM(CINDemandeur))) < 12
)
"""
cursor.execute(update_cindemandeur, codique)
print(f"🟢 {cursor.rowcount} lignes corrigées pour CINDemandeur.")
conn_cep.commit()

# === 5️⃣ Si DateCINDemandeur est NULL → 1990-01-01 ===
update_datecindemandeur = """
UPDATE dbo.TblLivret
SET DateCINDemandeur = '1990-01-01'
WHERE 
    LEFT(NLivret, 5) = ? AND
    (DateCINDemandeur IS NULL)
"""
cursor.execute(update_datecindemandeur, codique)
conn_cep.commit()
print(f"🟢 {cursor.rowcount} lignes mises à jour pour DateCINDemandeur.")

# === 6️⃣ Si DatenaissCLI est NULL → DateOuverture ===
update_datenaiss = """
UPDATE dbo.TblLivret
SET DatenaissCLI = DateOuverture
WHERE 
    LEFT(NLivret, 5) = ? AND
    (DatenaissCLI IS NULL)
"""
cursor.execute(update_datenaiss, codique)
conn_cep.commit()
print(f"🟢 {cursor.rowcount} lignes mises à jour pour DatenaissCLI.")

# === Mise à jour DateSolde si NULL → DateOuverture ===
update_datesolde = """
UPDATE dbo.TblLivret
SET DateSolde = DateOuverture
WHERE LEFT(NLivret, 5) = ?
  AND (DateSolde IS NULL)
"""
cursor.execute(update_datesolde, codique)
rows = cursor.rowcount
conn_cep.commit()
print(f"🟢 {rows} lignes mises à jour pour DateSolde.")


# === 🔹 Nettoyage complet de Nationalité ===
# 🔹 Nettoyage de Nationalité pour retirer les espaces, tabulations, retours chariot
clean_nationalite = """
UPDATE dbo.TblLivret
SET Nationalité = LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(Nationalité, CHAR(9), ''), CHAR(10), ''), CHAR(13), '')))
WHERE LEFT(NLivret, 5) = ?
"""
cursor.execute(clean_nationalite, codique)
conn_cep.commit()

# 🔹 Mise à jour si Nationalité vide ou non numérique
update_nationalite = """
UPDATE dbo.TblLivret
SET Nationalité = '22'
WHERE LEFT(NLivret, 5) = ?
  AND (
        Nationalité IS NULL
        OR LTRIM(RTRIM(Nationalité)) = ''
        OR TRY_CAST(Nationalité AS INT) IS NULL
  )
"""
cursor.execute(update_nationalite, codique)
conn_cep.commit()
print(f"🟢 {cursor.rowcount} lignes mises à jour pour Nationalité = 22")


# === 🔹 Nettoyage préalable de TypeClient et des colonnes utilisées ===
clean_columns = """
UPDATE dbo.TblLivret
SET NomCLI = LTRIM(RTRIM(NomCLI)),
    CINCli = LTRIM(RTRIM(CINCli)),
    Siège = LTRIM(RTRIM(Siège)),
    NomCJoint = LTRIM(RTRIM(NomCJoint)),
    QlteDemandeur = LTRIM(RTRIM(QlteDemandeur)),
    TypeClient = LTRIM(RTRIM(ISNULL(TypeClient, '')))
WHERE LEFT(NLivret, 5) = ?
"""
cursor.execute(clean_columns, codique)
conn_cep.commit()


# 🔹 Mise à jour NumTel si non numérique → NULL
update_numtel = """
UPDATE dbo.TblLivret
SET NumTel = NULL
WHERE LEFT(NLivret, 5) = ?
  AND (NumTel IS NOT NULL AND TRY_CAST(NumTel AS BIGINT) IS NULL)
"""
cursor.execute(update_numtel, codique)
conn_cep.commit()
print(f"🟢 {cursor.rowcount} lignes mises à jour pour NumTel = NULL")


# === 🔹 Mise à jour unique de TypeClient avec CASE ===
update_typeclient = """
UPDATE dbo.TblLivret
SET TypeClient = CASE
    -- Client normal complet
    WHEN NomCLI IS NOT NULL AND NomCLI <> '' 
         AND CINCli IS NOT NULL AND CINCli <> ''
         AND DateCIN IS NOT NULL
         AND ([Siège] IS NULL OR [Siège] = '')
    THEN '12'
    
    -- Client entreprise
    WHEN NomCLI IS NOT NULL AND NomCLI <> '' 
         AND ([Siège] IS NOT NULL AND [Siège] <> '')
    THEN '20'
    
    -- Client particulier incomplet
    WHEN NomCLI IS NOT NULL AND NomCLI <> ''
         AND (CINCli IS NULL OR CINCli = '')
         AND (DateCIN IS NULL)
         AND ([Siège] IS NULL OR [Siège] = '')
         AND (QlteDemandeur IS NULL OR UPPER(QlteDemandeur) <> 'TITULAIRE')
    THEN '11'
    
    -- Compte joint
    WHEN NomCJoint IS NOT NULL AND NomCJoint <> ''
    THEN 'CON'
    
    -- Sinon on conserve l'existant
    ELSE '12'
END
WHERE LEFT(NLivret, 5) = ?
"""
cursor.execute(update_typeclient, codique)
conn_cep.commit()
print(f"🟢 {cursor.rowcount} lignes mises à jour pour TypeClient (mise à jour unique sécurisée)")





# for key, query in updates_type_client.items():
#     cursor.execute(query, codique)
#     print(f"🟢 {cursor.rowcount} lignes mises à jour pour TypeClient = {key}")
# conn_cep.commit()


# === 8️⃣ Récupération de toutes les données ===
query_cep = "SELECT * FROM dbo.TblLivret WHERE LEFT(NLivret, 5) = ?"
df_cep = pd.read_sql(query_cep, conn_cep, params=[codique])
print(f"📊 {len(df_cep)} lignes récupérées pour le codique {codique}.\n")

# === 🔹 Nettoyage des champs ===
def nettoyer_champs(df):
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].astype(str).apply(lambda x: re.sub(r'[\r\n]+', ' ', x).strip())
    return df

# === 🔹 Harmonisation ===
def harmoniser_dataframe(df, colonnes_source, slug):
    colonnes_cibles = {col: pd.NA for col in [
        'n_livret', 'nom_cli', 'prenoms_cli', 'date_naiss_cli', 'lieu_naiss_cli',
        'adresse_cli', 'code_postale', 'cin_cli', 'date_cin', 'code_prof', 'nationalite',
        'nom_pere_cli', 'nom_mere_cli', 'autres', 'date_ouverture', 'code_agent',
        'code_chef_etbs', 'nom_mandataire', 'prenoms_mandataire', 'domicile_mandataire',
        'qualite_mandataire', 'siege', 'solde', 'date_solde', 'pv', 'code_agence',
        'nom_agence', 'nom_prenom_demandeur', 'adr_demandeur', 'cin_demandeur',
        'date_cin_demandeur', 'qlte_demandeur', 'type_client', 'nom_transfert', 'cloture',
        'capitaliser', 'nom_c_joint', 'prenoms_c_joint', 'cin_c_joint', 'date_cin_c_joint',
        'adr_c_joint', 'compte_joint', 'conv_solde', 'conv_pv', 'solde_prec', 'num_tel', 'kodik'
    ]}
    df.rename(columns=colonnes_source, inplace=True)
    for col in colonnes_cibles:
        if col not in df.columns:
            df[col] = colonnes_cibles[col]
    df['slug'] = slug
    return df[list(colonnes_cibles.keys()) + ['slug']]

# === Correspondance des colonnes ===
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

# === 9️⃣ Nettoyage + Export CSV ===
df_final = harmoniser_dataframe(df_cep, colonnes_cep, 'TL')
df_final = nettoyer_champs(df_final)

nb_lignes = len(df_final)
if nb_lignes == 0:
    print("⚠️ Aucune donnée trouvée pour ce codique.")
else:
    nb_fichiers = math.ceil(nb_lignes / 10000)
    for i in range(nb_fichiers):
        start = i * 10000
        end = min(start + 10000, nb_lignes)
        bloc = df_final.iloc[start:end]
        fichier = f"initialisation_{codique}_part{i+1}.csv"
        bloc.to_csv(fichier, index=False, encoding='utf-8-sig', lineterminator='\n')
        print(f"✅ Bloc {i+1}/{nb_fichiers} exporté ({len(bloc)} lignes) → {fichier}")

conn_cep.close()
