import pandas as pd
import numpy as np
from ..config import get_city_name

def analyze_user(df: pd.DataFrame) -> dict:
    """
    Perform user analysis: Segmentation, VIP comparison, Geography.
    """
    results = {}
    
    # --- 1. RFM Segmentation ---
    # R: Not explicitly available as "days since last order", using a proxy or skip R logic if not strictly doable.
    # Spec says: use qcut on total_amt_30 (M), ord_30 (F).
    # Let's simplify to 5 segments based on M and F for demonstration if R is missing.
    # Actually doc says: "提取用户的 Recency...". But we only have `times` (current request time) and lists of history.
    # We will use M and F to segment.
    
    # Calculate Scores
    # qcut needs unique bins, 'rank' method handles duplicates
    try:
        df['f_score'] = pd.qcut(df['ord_30'].rank(method='first'), q=3, labels=[1, 2, 3])
        df['m_score'] = pd.qcut(df['total_amt_30'].rank(method='first'), q=3, labels=[1, 2, 3])
    except Exception:
        # Fallback if too little data
        df['f_score'] = 1
        df['m_score'] = 1

    def assign_segment(row):
        f, m = row['f_score'], row['m_score']
        is_vip = row['is_supervip'] == 1
        
        if is_vip and m == 3: return "👑 超级VIP用户"
        if m == 3 and f >= 2: return "💎 潜力优质用户"
        if m == 2 or f == 3: return "💰 大众活跃用户"
        if m == 1 and f == 1: return "🔄 流失风险用户"
        return "👤 一般用户"

    df['segment'] = df.apply(assign_segment, axis=1)
    
    # 1.1 Distribution
    seg_counts = df['segment'].value_counts(normalize=True) * 100
    seg_data = []
    colors = {
        "👑 超级VIP用户": "#faad14",
        "💎 潜力优质用户": "#1890ff",
        "💰 大众活跃用户": "#52c41a",
        "👤 一般用户": "#8c8c8c",
        "🔄 流失风险用户": "#f5222d"
    }
    
    for name, val in seg_counts.items():
        seg_data.append({
            "name": name, 
            "value": round(val, 2),
            "color": colors.get(name, "#333")
        })
    results['segment_distribution'] = seg_data
    
    # 1.2 Average Consumption
    avg_cons = df.groupby('segment')['total_amt_30'].mean().round(2)
    # Sort by custom order
    order = ["👑 超级VIP用户", "💎 潜力优质用户", "💰 大众活跃用户", "👤 一般用户", "🔄 流失风险用户"]
    ordered_values = []
    ordered_cats = []
    for cat in order:
        if cat in avg_cons.index:
            ordered_cats.append(cat.replace("👑 ", "").replace("💎 ", "").replace("💰 ", "").replace("👤 ", "").replace("🔄 ", ""))
            ordered_values.append(avg_cons[cat])
            
    results['segment_avg_consumption'] = {
        "categories": ordered_cats,
        "values": ordered_values
    }
    
    # --- 2. VIP Comparison ---
    vip_grp = df.groupby('is_supervip')
    
    # Metrics
    # Click Rate: label mean * 100
    click_rate = vip_grp['label'].mean() * 100
    # Conversion (User level avg): ord_30 / ctr_30 mean
    # Avoid div by zero
    temp_df = df.copy()
    temp_df['cvr'] = temp_df.apply(lambda r: r['ord_30']/r['ctr_30'] if r['ctr_30']>0 else 0, axis=1)
    conv_rate = temp_df.groupby('is_supervip')['cvr'].mean() * 100
    
    avg_price = vip_grp['avg_price'].mean()
    orders_30 = vip_grp['ord_30'].mean()
    
    results['vip_comparison'] = {
        "categories": ["点击率(%)", "转化率(%)", "平均客单价(元)", "30天下单次数"],
        "vip": [
            round(click_rate.get(1, 0), 2),
            round(conv_rate.get(1, 0), 2),
            round(avg_price.get(1, 0), 2),
            round(orders_30.get(1, 0), 2)
        ],
        "normal": [
            round(click_rate.get(0, 0), 2),
            round(conv_rate.get(0, 0), 2),
            round(avg_price.get(0, 0), 2),
            round(orders_30.get(0, 0), 2)
        ]
    }
    
    # --- 3. City Distribution ---
    city_counts = df['visit_city'].value_counts().head(20)
    city_data = []
    for city_id, count in city_counts.items():
        city_data.append({
            "name": get_city_name(city_id),
            "value": int(count)
        })
    results['city_distribution'] = city_data
    
    return {"user": results}
