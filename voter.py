import re

raw_data = """
[1] nzni752039 | [2] nzni752039 | [3] KKG3112315
ନାମ : ଅଭିଜିତ ପ୍ରଧାନ ନାମ : ଚରଣ ମୁୁଁ ନାମ : ପାର୍ବତୀ ମୁମୁଁ
ପିତାଙ୍କ ନାମ: ଲକ୍ଷ୍ମୀଧର ପ୍ରଧାନ ପିତାଙ୍କ ନାମ: ବିଧାନ ମୁମୁଁ ସ୍ଵାମୀଙ୍କ ନାମ: ରାଜ ମୁମୁଁ
ଘର ନଂ : 000 ଫଟୋ ଉପଲବ୍ଧ | | ଘର ନଂ : 2 ଫଟୋ ଉପଲବ୍ଧ | ) ଘର ନଂ : 2 ଫଟୋ ଉପ୍ଲଷା
ବୟସ : 21 ଲିଙ୍ଗ : ପୁରୁଷ ବୟସ : 58 ଲିଙ : ପୁରୁଷ ବୟସ : 56 ଲିଙଂ : ସୀ
"""

lines = raw_data.strip().split("\n")

# Extract IDs
ids = re.findall(r'\b[nzNkK][a-z0-9/]+', lines[0])
person_count = len(ids)

def extract_fields(line, pattern, count=person_count):
    matches = re.findall(pattern, line)
    matches = [m.strip() for m in matches]
    while len(matches) < count:
        matches.append('')
    return matches[:count]

# Extract all fields
names = extract_fields(lines[1], r'ନାମ\s*:\s*([^ନାମ:|]+)')
father_spouse = extract_fields(lines[2], r'(?:ପିତାଙ୍କ ନାମ|ସ୍ଵାମୀଙ୍କ ନାମ)\s*:\s*([^ପିତାଙ୍କ:ସ୍ଵାମୀଙ୍କ|]+)')

# Extract house numbers and photo availability
house_nos = []
photo_avail = []
parts = re.split(r'\|+', lines[3])
for part in parts:
    part = part.strip()
    if not part:
        continue
    h = re.search(r'ଘର ନଂ\s*:\s*(\d+)', part)
    if h:
        house_nos.append('0' if h.group(1) == '000' else h.group(1))
    photo_avail.append('Y' if 'ଫଟୋ ଉପଲବ୍ଧ' in part else 'N')

# Ensure correct count
house_nos = house_nos[:person_count]
photo_avail = photo_avail[:person_count]

# Extract ages and genders
ages = extract_fields(lines[4], r'ବୟସ\s*:\s*(\d+)')
genders = extract_fields(lines[4], r'ଲିଙ[ଗଂ]?\s*:\s*([^ବୟସ|]+)')

# Map genders to M/F
gender_map = {'ପୁରୁଷ': 'M', 'ସୀ': 'F'}
genders = [gender_map.get(g.strip(), g) for g in genders]

# Print the formatted output
for i in range(person_count):
    print(f"{ids[i]}\t{names[i]}\t{father_spouse[i]}\t{house_nos[i]}\t{photo_avail[i]}\t{genders[i]}")