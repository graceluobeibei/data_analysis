#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
@Author: Jupiter.Lin
@CreateDate: 2026-01-24
@Description: Inject generated JSON data into the HTML report
@Version: 2.0
"""

import json
import os
import sys

# Add src to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../")))
from main.config import OUTPUT_PATH

def load_json_files():
    """
    从output目录加载所有拆分的JSON文件
    
    返回：
    ------
    dict
        合并后的数据字典
    """
    json_files = {
        'metrics': 'metrics.json',
        'user': 'user.json',
        'product': 'product.json',
        'behavior': 'behavior.json',
        'summary': 'summary.json'
    }
    
    data = {}
    
    for key, filename in json_files.items():
        filepath = os.path.join(OUTPUT_PATH, filename)
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                file_data = json.load(f)
                # 如果JSON文件的顶层键就是分类名(如 {"metrics": {...}})，则提取内容
                if key in file_data:
                    data[key] = file_data[key]
                elif key == 'summary':
                    # summary.json 的结构是 {"summary_table": [...]}
                    data['summary_table'] = file_data.get('summary_table', [])
                else:
                    data[key] = file_data
            print(f"✅ 成功加载: {filename}")
        except FileNotFoundError:
            print(f"⚠️  文件不存在: {filename}, 使用空数据")
            if key == 'summary':
                data['summary_table'] = []
            else:
                data[key] = {}
        except json.JSONDecodeError as e:
            print(f"❌ JSON解析错误 ({filename}): {e}")
            if key == 'summary':
                data['summary_table'] = []
            else:
                data[key] = {}
    
    return data

def inject_data():
    """
    将JSON数据注入到HTML模板中生成静态报告
    """
    html_template_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../presentation.html"))
    output_html_path = os.path.join(OUTPUT_PATH, "report_static.html")
    
    print(f"正在从 {OUTPUT_PATH} 加载JSON数据...")
    data = load_json_files()

    print(f"正在读取HTML模板: {html_template_path}...")
    try:
        with open(html_template_path, 'r', encoding='utf-8') as f:
            html_content = f.read()
    except Exception as e:
        print(f"❌ 读取HTML模板失败: {e}")
        return

    # 构建JavaScript代码
    js_code = f"""
// ========== 数据定义 (Generated) ==========

// 核心指标
const metricsData = {json.dumps(data.get('metrics', {}), ensure_ascii=False, indent=2)};

// 用户分层数据
const userSegmentData = {json.dumps(data.get('user', {}).get('segment_distribution', []), ensure_ascii=False, indent=2)};
const userValueData = {json.dumps(data.get('user', {}).get('segment_avg_consumption', {}), ensure_ascii=False, indent=2)};
const vipData = {json.dumps(data.get('user', {}).get('vip_comparison', {}), ensure_ascii=False, indent=2)};
const cityData = {json.dumps(data.get('user', {}).get('city_distribution', []), ensure_ascii=False, indent=2)};

// 商品数据
const productData = {json.dumps(data.get('product', {}).get('top_products', {}), ensure_ascii=False, indent=2)};
const categoryData = {json.dumps(data.get('product', {}).get('category_distribution', []), ensure_ascii=False, indent=2)};
const priceData = {json.dumps(data.get('product', {}).get('price_analysis', {}), ensure_ascii=False, indent=2)};
const rankData = {json.dumps(data.get('product', {}).get('rank_effect', {}), ensure_ascii=False, indent=2)};

// 行为数据
const hourlyData = {json.dumps(data.get('behavior', {}).get('hourly_trend', {}), ensure_ascii=False, indent=2)};
const weekdayData = {json.dumps(data.get('behavior', {}).get('weekday_comparison', {}), ensure_ascii=False, indent=2)};
const funnelData = {json.dumps(data.get('behavior', {}).get('conversion_funnel', []), ensure_ascii=False, indent=2)};
const timeCategoryData = {json.dumps(data.get('behavior', {}).get('time_category_preference', {}), ensure_ascii=False, indent=2)};

// 明细表数据
const summaryTableData = {json.dumps(data.get('summary_table', []), ensure_ascii=False, indent=2)};

// Update Metrics on Page
function updateMetrics() {{
    const m = metricsData;
    if (!m) return;
    
    const metrics = m;
    
    // Helper to find metric card by label text
    const cards = document.querySelectorAll('.metric-card');
    cards.forEach(card => {{
        const label = card.querySelector('.metric-label').textContent;
        const valueEl = card.querySelector('.metric-value');
        
        if (label.includes('总曝光量')) valueEl.textContent = metrics.total_impressions_unit ? metrics.total_impressions + metrics.total_impressions_unit : metrics.total_impressions;
        if (label.includes('总点击量')) valueEl.textContent = metrics.total_clicks_unit ? metrics.total_clicks + metrics.total_clicks_unit : metrics.total_clicks;
        if (label.includes('全局点击率')) valueEl.textContent = metrics.global_ctr + '%';
        if (label.includes('转化率')) valueEl.textContent = metrics.global_cvr + '%';
        if (label.includes('平均客单价')) valueEl.textContent = '¥' + metrics.avg_price;
        if (label.includes('活跃用户数')) valueEl.textContent = metrics.active_users_unit ? metrics.active_users + metrics.active_users_unit : metrics.active_users;
    }});
    
    // Update Header Info
    const headerP = document.querySelector('.header p:last-child');
    if (headerP) {{
        headerP.textContent = `分析时段: 2022年4月1日 | 数据规模: ${{metrics.total_impressions}}条记录`;
    }}
}}

// Initialize
window.addEventListener('DOMContentLoaded', () => {{
    updateMetrics();
}});
"""

    # 替换HTML中的数据部分
    # 查找 "// ========== 数据定义 ==========" 和 "// ========== 图表渲染函数 ==========" 之间的内容
    start_marker = "// ========== 数据定义 =========="
    end_marker = "// ========== 图表渲染函数 =========="
    
    start_idx = html_content.find(start_marker)
    end_idx = html_content.find(end_marker)
    
    if start_idx != -1 and end_idx != -1:
        new_html = html_content[:start_idx] + js_code + "\n\n" + html_content[end_idx:]
        
        with open(output_html_path, 'w', encoding='utf-8') as f:
            f.write(new_html)
        print(f"✅ 报告生成成功: {output_html_path}")
    else:
        print("❌ 未找到HTML模板中的数据标记区域")

if __name__ == "__main__":
    inject_data()
