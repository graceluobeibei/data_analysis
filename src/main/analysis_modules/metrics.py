import pandas as pd

def calculate_metrics(df: pd.DataFrame) -> dict:
    """
    Calculate core KPI metrics.
    """
    total_impressions = len(df)
    total_clicks = int(df['label'].sum())
    
    # Avoid division by zero
    global_ctr = (total_clicks / total_impressions * 100) if total_impressions > 0 else 0
    
    # CVR: Total Orders / Total Clicks (Approximation using ord_30 is tricky because ord_30 is user-level history, 
    # but the requirement says "global_cvr: 28.5%". 
    # For this specific dataset, label=1 means click. We don't have an explicit "order" label for this specific interaction.
    # However, the requirement doc says "Global CVR = 订单数 / 点击数".
    # Since we lack a direct 'is_ordered' label for the interaction, we might use a proxy or placeholder logic based on doc.
    # Requirement data spec says: "global_cvr = (ord_30 / ctr_30) 的平均值" (Average of user historical CVR).
    # Let's stick to the spec logic:
    
    # Filter users with > 0 clicks to avoid inf
    valid_users = df[df['ctr_30'] > 0]
    if len(valid_users) > 0:
        # User level CVR
        user_cvr = valid_users['ord_30'] / valid_users['ctr_30']
        global_cvr = user_cvr.mean() * 100
    else:
        global_cvr = 0
        
    avg_price = df['avg_price'].mean()
    active_users = df['user_id'].nunique()
    
    return {
        "metrics": {
            "total_impressions": total_impressions,
            "total_impressions_unit": "M" if total_impressions > 1000000 else "",
            "total_clicks": round(total_clicks / 1000000, 2) if total_clicks > 1000000 else total_clicks,
            "total_clicks_unit": "M" if total_clicks > 1000000 else "",
            "global_ctr": round(global_ctr, 2),
            "global_cvr": round(global_cvr, 2),
            "avg_price": round(avg_price, 2),
            "active_users": round(active_users / 1000000, 2) if active_users > 1000000 else active_users,
            "active_users_unit": "M" if active_users > 1000000 else ""
        }
    }
