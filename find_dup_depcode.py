# python
import csv
from collections import defaultdict
import sys
from pathlib import Path

path = Path(r"HrImport.csv")

if not path.exists():
    print(f"File not found: {path}", file=sys.stderr)
    sys.exit(1)

map_names = defaultdict(set)
rows = []

with path.open(newline='', encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    for lineno, r in enumerate(reader, start=2):
        code = (r.get('DEP_CODE') or '').strip()
        name = (r.get('DEP_NAME') or '').strip()
        map_names[code].add(name)
        rows.append((lineno, code, name, r))

# print duplicates
found = []
for code, names in sorted(map_names.items()):
    if code and len(names) > 1:
        found.append((code, names))

if not found:
    print('No DEP_CODE with multiple DEP_NAME found')
else:
    for code, names in found:
        print('\nDEP_CODE:', code)
        print('  distinct DEP_NAMEs (count={}):'.format(len(names)))
        for n in sorted(names):
            print('    -', n)
        print('  sample rows:')
        cnt = 0
        for lineno, c, n, r in rows:
            if c == code:
                print('    line', lineno, '| DEP_NO=', r.get('DEP_NO'), '| EMP_ID=', r.get('EMP_ID'), '| EMP_NAME=', r.get('EMP_NAME'), '| DEP_NAME=', n)
                cnt += 1
                if cnt >= 5:
                    break