import pandas as pd
import numpy as np
from ..config import get_category_name

def analyze_product(df: pd.DataFrame) -> dict:
    results = {}
    
    # --- 1. Top Products ---
    # Aggregate clicks and impressions (count of rows) per item
    prod_stats = df.groupby('item_id').agg({
        'label': 'sum',
        'user_id': 'count'
    }).rename(columns={'label': 'clicks', 'user_id': 'impressions'})
    
    prod_stats['ctr'] = (prod_stats['clicks'] / prod_stats['impressions'] * 100).round(2)
    top_30 = prod_stats.sort_values('clicks', ascending=False).head(30)
    
    results['top_products'] = {
        "items": [f"Item_{i[:6]}" for i in top_30.index], # Truncate hash for display
        "clicks": top_30['clicks'].tolist(),
        "ctr": top_30['ctr'].tolist()
    }
    
    # --- 2. Category Distribution ---
    cat_stats = df.groupby('category_1_id')['label'].sum()
    cat_data = []
    # Sort by clicks
    cat_stats = cat_stats.sort_values(ascending=False).head(7) # Top 7 + others ideally, but simple top 7 here
    for cat_id, clicks in cat_stats.items():
        cat_data.append({
            "name": get_category_name(cat_id),
            "value": int(clicks)
        })
    results['category_distribution'] = cat_data
    
    # --- 3. Price Analysis ---
    # Binning
    bins = [0, 20, 40, 60, 80, float('inf')]
    labels = ["<20元", "20-40元", "40-60元", "60-80元", ">80元"]
    df['price_bin'] = pd.cut(df['avg_price'], bins=bins, labels=labels)
    
    price_grp = df.groupby('price_bin', observed=False)
    
    # Click rate: sum(label) / count
    p_clicks = (price_grp['label'].mean() * 100).round(2)
    
    # Conversion rate: avg of (ord_30/ctr_30) per bin
    # Note: Using user's conversion ability as proxy for product conversion in that price range
    # Ideally should use is_ordered label but we don't have it.
    temp_df = df.copy()
    temp_df['cvr'] = temp_df.apply(lambda r: r['ord_30']/r['ctr_30'] if r['ctr_30']>0 else 0, axis=1)
    # Re-assign price bin to temp_df
    temp_df['price_bin'] = pd.cut(temp_df['avg_price'], bins=bins, labels=labels)
    p_cvr = (temp_df.groupby('price_bin', observed=False)['cvr'].mean() * 100).round(2)
    
    results['price_analysis'] = {
        "ranges": labels,
        "clicks": [p_clicks.get(l, 0) for l in labels],
        "conversion": [p_cvr.get(l, 0) for l in labels]
    }
    
    # --- 4. Rank Effect ---
    # Rank 7
    rank_bins = [0, 5, 10, 20, 50, 100, float('inf')]
    rank_labels = ["TOP 1-5", "TOP 6-10", "TOP 11-20", "TOP 21-50", "TOP 51-100", "100+"]
    df['rank_bin'] = pd.cut(df['rank_7'], bins=rank_bins, labels=rank_labels)
    
    rank_grp = df.groupby('rank_bin', observed=False)
    r_ctr = (rank_grp['label'].mean() * 100).round(2)
    r_imp = (rank_grp['user_id'].count() / 1000000).round(2) # In Millions
    
    results['rank_effect'] = {
        "positions": rank_labels,
        "ctr": [r_ctr.get(l, 0) for l in rank_labels],
        "impressions": [r_imp.get(l, 0) for l in rank_labels]
    }
    
    return {"product": results}
