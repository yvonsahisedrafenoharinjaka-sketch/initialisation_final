import pyodbc
import pandas as pd


server = '154.126.56.88,1700'
user = 'sa'
password = '123P@om@456'
driver = '{ODBC Driver 17 for SQL Server}'
database = 'CEP'

conn = pyodbc.connect(
    f'DRIVER={driver};SERVER={server};UID={user};PWD={password};DATABASE={database}'
)
conn.autocommit = True
cursor = conn.cursor()

kodik = input("Entrez le Numéro Codique (ex: 10101) ou vide pour tous : ").strip()
where_clause = f"WHERE LEFT(NLivret,5)='{kodik}'" if kodik else ""

print("\n🔄 Mise à jour du solde des livrets en cours...\n")


query_livrets = f"SELECT NLivret, Solde FROM dbo.TblLivret {where_clause};"
cursor.execute(query_livrets)
livrets = cursor.fetchall()

updated_livrets = []

for nl, ancien_solde in livrets:
    
    cursor.execute("""
        SELECT SUM(
            CASE 
                WHEN CodeOperation IN (1,4) THEN Montant
                WHEN CodeOperation = 2 THEN -Montant
                ELSE 0
            END
        ) AS CalculSolde
        FROM dbo.TblMVT
        WHERE CodeLivret = ?
    """, nl)
    row = cursor.fetchone()
    nouveau_solde = row[0] if row[0] is not None else 0

    print(f"NLivret: {nl} | Ancien Solde: {ancien_solde:.2f} → Nouveau Solde: {nouveau_solde:.2f}")

    
    cursor.execute("UPDATE dbo.TblLivret SET Solde=? WHERE NLivret=?", nouveau_solde, nl)

   
    if ancien_solde != nouveau_solde:
        updated_livrets.append((nl, ancien_solde, nouveau_solde))


if updated_livrets:
    df_changed = pd.DataFrame(updated_livrets, columns=["NLivret", "AncienSolde", "NouveauSolde"])
    filename = f"TblLivret_MAJ{'_'+kodik if kodik else ''}.csv"
    df_changed.to_csv(filename, index=False, encoding='utf-8-sig')
    print(f"\n📦 Export des livrets modifiés → {filename}")
else:
    print("\nAucun livret n’a subi de modification.")

cursor.close()
conn.close()
print("\n✅ Opération terminée.")