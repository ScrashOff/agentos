from pathlib import Path
import subprocess, tarfile
root=Path(__file__).resolve().parents[1]
names=subprocess.check_output(['git','ls-files','--cached','--others','--exclude-standard'],cwd=root,text=True).splitlines()
output=root/'.local'/'agentos-release.tar.gz'
with tarfile.open(output,'w:gz') as tar:
 for name in names:
  p=root/name
  if p.is_file() and not name.startswith(('.env','.local','node_modules/')) and '__pycache__' not in p.parts:
   tar.add(p,arcname=name,recursive=False)
print(str(output))
