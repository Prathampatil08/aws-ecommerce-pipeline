content = open('src/data_generator/generate_data.py', encoding='utf-8').read()

content = content.replace('print("\\n\\U0001f680 Batch mode ▒ generating data ▒")', 'print("\\nBatch mode - generating data ...")')
content = content.replace('print("\\n\\U0001f680 Stream mode', 'print("\\nStream mode')
content = content.replace('print("\\n✅ Batch upload complete.")', 'print("\\nBatch upload complete.")')
content = content.replace('print("\\n✅ Streaming complete.")', 'print("\\nStreaming complete.")')
content = content.replace('print(f"  ✔ Wrote', 'print(f"  Wrote')
content = content.replace('print(f"  ✔ Uploaded', 'print(f"  Uploaded')
content = content.replace('print(f"  ✔ Sent', 'print(f"  Sent')
content = content.replace('print(f"  → Total', 'print(f"  Total')

open('src/data_generator/generate_data.py', 'w', encoding='utf-8').write(content)
print('Fixed successfully')
