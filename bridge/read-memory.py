"""Scoped, read-only import of Harness notes already destined for Recall."""
import json, re, sqlite3, sys, unicodedata
project,query=sys.argv[1:3]
def words(text):
    return set(re.findall(r'[a-z0-9]{3,}', ''.join(c for c in unicodedata.normalize('NFKD',text.lower()) if not unicodedata.combining(c))))
db=sqlite3.connect('file:/var/lib/deepseek-harness/memory/notes.sqlite?mode=ro',uri=True)
db.row_factory=sqlite3.Row
tokens=words(query)
rows=[dict(r) for r in db.execute('SELECT id,description,body,project,kind,created,synced FROM notes WHERE project=? ORDER BY created DESC LIMIT 500',(project,))]
ranked=sorted(rows,key=lambda r:len(tokens & words(r['description']+' '+r['body'])),reverse=True)
print(json.dumps({'items':[r for r in ranked if tokens & words(r['description']+' '+r['body'])][:8],'source':'harness-recall-notes','project':project}))
