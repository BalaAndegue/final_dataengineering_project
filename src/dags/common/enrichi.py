import os
import pandas as pd

def agg_data(date):
    product_path = os.path.join("data", "clean_data", "products", str(date.year), str(date.month), f"{str(date.day)}.csv")
    order_path = os.path.join("data", "clean_data", "orders", str(date.year), str(date.month), f"{str(date.day)}.csv")
    
    df_product = pd.read_csv(product_path)
    df_order = pd.read_csv(order_path)
    
    # Calculer la quantité commandée par produit
    commande = df_order.groupby(['product_id', 'order_date'])['quantity'].sum().reset_index()
    # Fusionner avec le stock initial
    stock_journalier = pd.merge(df_product, commande, how='left', left_on='product_id', right_on='product_id')
    stock_journalier['quantity'] = stock_journalier['quantity'].fillna(0)
    stock_journalier['stock_journalier'] = stock_journalier['stock'] - stock_journalier['quantity']
    stock_journalier['date'] = date.strftime("%Y-%m-%d")
    
    # Sélectionner les colonnes à exporter
    out = stock_journalier[['product_id', 'product_name', 'stock', 'quantity', 'stock_journalier', 'date']]
    
    out_path = os.path.join("data", "enrichi_data", "products", str(date.year), str(date.month), f"{str(date.day)}.csv")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    out.to_csv(out_path, index=False)
    print(f"Stock journalier sauvegardé dans : {out_path}")

if __name__ == "__main__":
    from datetime import datetime
    date_to_process = datetime.strptime("2024-05-10", "%Y-%m-%d")
    agg_data(date_to_process)

# if __name__ == "__main__":
#     from datetime import datetime
#     # Exemple d'exécution pour la date du 3 juin 2024
#     date_to_process = datetime.strptime("2024-05-10", "%Y-%m-%d")

#     #clean_clients_data(date_to_process)
#     #clean_products_data(date_to_process)
#     agg_data(date_to_process)