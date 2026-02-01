import pandas as pd
import numpy as np
from ..config import get_category_name

def analyze_behavior(df: pd.DataFrame) -> dict:
    results = {}
    
    # --- 1. Hourly Trend ---
    # Need hour extraction if not present
    if 'datetime' in df.columns:
        df['hour_extracted'] = df['datetime'].dt.hour
    elif 'hours' in df.columns:
        df['hour_extracted'] = df['hours']
    else:
        df['hour_extracted'] = 0
        
    hour_grp = df.groupby('hour_extracted')
    h_clicks = hour_grp['label'].sum()
    # Approx orders using CVR proxy: click * avg_cvr
    # Or just use sum of clicks * 0.285 (global cvr) for shape
    h_orders = (h_clicks * 0.285).astype(int) 
    
    results['hourly_trend'] = {
        "hours": [f"{i}:00" for i in range(24)],
        "clicks": [int(h_clicks.get(i, 0)) for i in range(24)],
        "orders": [int(h_orders.get(i, 0)) for i in range(24)]
    }
    
    # --- 2. Weekday vs Weekend ---
    if 'weekdays' in df.columns:
        # 0=Monday, ... 5=Sat, 6=Sun
        df['is_weekend'] = df['weekdays'].isin([5, 6])
        
        wk_grp = df.groupby('is_weekend')
        
        # Metrics: Clicks (K), Orders (K), Avg Price, CTR
        w_clicks = wk_grp['label'].sum() / 1000
        w_ctr = wk_grp['label'].mean() * 100
        w_price = wk_grp['avg_price'].mean()
        # Proxy orders
        w_orders = (wk_grp['label'].sum() * 0.285) / 1000
        
        results['weekday_comparison'] = {
            "metrics": ["点击数(千)", "订单数(千)", "平均客单价(元)", "CTR(%)"],
            "weekday": [
                round(w_clicks.get(False, 0), 1),
                round(w_orders.get(False, 0), 1),
                round(w_price.get(False, 0), 1),
                round(w_ctr.get(False, 0), 1)
            ],
            "weekend": [
                round(w_clicks.get(True, 0), 1),
                round(w_orders.get(True, 0), 1),
                round(w_price.get(True, 0), 1),
                round(w_ctr.get(True, 0), 1)
            ]
        }
    
    # --- 3. Funnel ---
    impressions = len(df)
    clicks = int(df['label'].sum())
    # Estimate orders: impressions * global_ctr * global_cvr
    # Real data: we don't have order label per row. 
    # Use global stats: 128M imp, 4.56M clicks (3.56%), ~1.3M orders (28.5% CVR)
    # Scale to current sample size
    orders = int(clicks * 0.285)
    add_to_cart = int(clicks * 0.4) # Mock ratio
    
    results['conversion_funnel'] = [
        {"name": "曝光", "value": impressions},
        {"name": "点击", "value": clicks},
        {"name": "加购", "value": add_to_cart},
        {"name": "下单", "value": orders}
    ]
    
    # --- 4. Time-Category Preference ---
    # Time periods
    def get_time_period(h):
        if 6 <= h <= 9: return "早餐(6-9)"
        if 11 <= h <= 13: return "午餐(11-13)"
        if 14 <= h <= 16: return "下午茶(14-16)"
        if 17 <= h <= 20: return "晚餐(17-20)"
        if 21 <= h <= 23 or 0 <= h <= 5: return "夜宵(21-24)"
        return "其他"
        
    df['time_period'] = df['hour_extracted'].apply(get_time_period)
    periods = ["早餐(6-9)", "午餐(11-13)", "下午茶(14-16)", "晚餐(17-20)", "夜宵(21-24)"]
    
    # Top 5 categories
    top_cats = df['category_1_id'].value_counts().head(5).index.tolist()
    cat_names = [get_category_name(c) for c in top_cats]
    
    matrix_data = []
    for p in periods:
        period_data = df[df['time_period'] == p]
        if len(period_data) == 0:
            matrix_data.append([0]*5)
            continue
            
        cat_counts = period_data['category_1_id'].value_counts(normalize=True) * 100
        row = []
        for c in top_cats:
            row.append(round(cat_counts.get(c, 0), 1))
        matrix_data.append(row)
        
    results['time_category_preference'] = {
        "times": periods,
        "categories": cat_names,
        "data": matrix_data
    }
    
    return {"behavior": results}
