import csv
from collections import defaultdict

print("=" * 50)
print("   E-COMMERCE PIPELINE ANALYTICS REPORT")
print("=" * 50)

# Revenue by Channel
print("\nREVENUE BY CHANNEL")
print("-" * 40)
channels = defaultdict(lambda: {'orders': 0, 'revenue': 0.0})
for r in csv.DictReader(open('data/gold_revenue.csv')):
    ch = r['channel']
    channels[ch]['orders']  += int(r['total_orders'])
    channels[ch]['revenue'] += float(r['gross_revenue'])
for ch, v in sorted(channels.items(), key=lambda x: x[1]['revenue'], reverse=True):
    print(f"  {ch:8s}  Orders: {v['orders']:>5}  Revenue: ${v['revenue']:>12,.2f}")

# Revenue by Country
print("\nREVENUE BY COUNTRY")
print("-" * 40)
rows = sorted(csv.DictReader(open('data/gold_geo.csv')),
              key=lambda x: float(x['revenue']), reverse=True)
for r in rows:
    print(f"  {r['country']:5s}  Orders: {r['orders']:>5}  Revenue: ${float(r['revenue']):>12,.2f}")

# Order Status Funnel
print("\nORDER STATUS FUNNEL")
print("-" * 40)
funnel = list(csv.DictReader(open('data/gold_funnel.csv')))
total  = sum(int(r['order_count']) for r in funnel)
for r in sorted(funnel, key=lambda x: int(x['order_count']), reverse=True):
    count = int(r['order_count'])
    pct   = count / total * 100
    bar   = '#' * int(pct / 2)
    print(f"  {r['status']:12s} {count:>5} orders ({pct:4.1f}%) {bar}")

# Summary
print("\nPIPELINE SUMMARY")
print("-" * 40)
all_orders = sum(v['orders'] for v in channels.values())
all_revenue = sum(v['revenue'] for v in channels.values())
print(f"  Total Orders  : {all_orders:,}")
print(f"  Total Revenue : ${all_revenue:,.2f}")
print(f"  Avg Order Val : ${all_revenue/all_orders:,.2f}")
print(f"  Countries     : {len(rows)}")
print(f"  Channels      : {len(channels)}")
print("\n  Pipeline: Bronze -> Silver -> Gold -> Analytics")
print("  Status  : COMPLETE")
print("=" * 50)