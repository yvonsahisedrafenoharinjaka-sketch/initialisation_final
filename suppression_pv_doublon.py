import pyodbc


server = '154.126.56.88,1700'
database = 'CEP'
user = 'sa'
password = '123P@om@456'
driver = '{ODBC Driver 17 for SQL Server}'


codique = input("Entrez le Numéro Codique (ex: 10101) : ").strip()

if not codique.isdigit() or len(codique) < 3:
    print("❌ Entrée invalide. Veuillez entrer un numéro codique numérique (ex: 51641).")
    exit()


try:
    conn = pyodbc.connect(
        f"DRIVER={driver};SERVER={server};DATABASE={database};UID={user};PWD={password}"
    )
    cursor = conn.cursor()
except Exception as e:
    print("❌ Erreur de connexion :", e)
    exit()


print(f"\n🔍 Recherche des doublons pour les livrets commençant par {codique}...\n")

check_query = f"""
SELECT CodeLivret, DateOperation, COUNT(*) AS nb_doublons
FROM dbo.TblMVT
WHERE CodeOperation = 1
  AND LEFT(CAST(CodeLivret AS VARCHAR(20)), {len(codique)}) = '{codique}'
GROUP BY CodeLivret, DateOperation
HAVING COUNT(*) > 1
ORDER BY CodeLivret, DateOperation;
"""
cursor.execute(check_query)
duplicates = cursor.fetchall()

if not duplicates:
    print("✅ Aucun doublon trouvé — la table est déjà propre.")
else:
    print(f"⚠️ {len(duplicates)} groupes de doublons détectés.\n")
    print("=== Détails des doublons détectés ===")
    print(f"{'CodeLivret':<15} {'DateOperation':<20} {'Nb Doublons':<12}")
    print("-" * 50)
    for row in duplicates:
        print(f"{row.CodeLivret:<15} {row.DateOperation.strftime('%Y-%m-%d %H:%M:%S'):<20} {row.nb_doublons:<12}")
    print("-" * 50)

    confirm = input("\nSouhaitez-vous supprimer ces doublons ? (o/n) : ").strip().lower()
    if confirm != 'o':
        print("🟡 Suppression annulée par l'utilisateur.")
        cursor.close()
        conn.close()
        exit()

    delete_query = f"""
    WITH doublons AS (
        SELECT 
            CodeMvt,
            ROW_NUMBER() OVER (
                PARTITION BY CodeLivret, DateOperation 
                ORDER BY CodeMvt
            ) AS rn
        FROM dbo.TblMVT
        WHERE CodeOperation = 1
          AND LEFT(CAST(CodeLivret AS VARCHAR(20)), {len(codique)}) = '{codique}'
    )
    DELETE FROM doublons WHERE rn > 1;
    """
    try:
        cursor.execute(delete_query)
        conn.commit()
        print("\n✅ Doublons supprimés avec succès.")
    except Exception as e:
        print("❌ Erreur lors de la suppression :", e)
        conn.rollback()

    cursor.execute(check_query)
    remaining = cursor.fetchall()
    if not remaining:
        print("✅ Vérification OK : aucun doublon restant.")
    else:
        print(f"⚠️ {len(remaining)} doublons persistent après suppression.")


cursor.close()
conn.close()
print("\n🔒 Connexion fermée.")