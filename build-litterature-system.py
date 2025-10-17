import json
import yaml
import pandas as pd
from pathlib import Path
import re
from datetime import datetime

# === CONFIGURATION ===
DATA_DIR = Path("data")
NOTES_DIR = Path("notes")
INDEX_DIR = Path("index")

ZOTERO_JSON = DATA_DIR / "biblio.json"
CSV_INDEX = INDEX_DIR / "literature_index.csv"

TYPE_FOLDERS = {
    "book": "books",
    "article-journal": "articles",
    "paper-conference": "conferences",
}

# These are your qualitative analysis fields
CUSTOM_FIELDS = [
    "Classement",
    "Champs de recherche",
    "Objet de recherche",
    "Niveau d'analyse",
    "Idée centrale",
    "Méthodologie",
    "Type de papier",
    "Taille échantillon",
    "Zone géographique",
    "Hypothèses (explicites / implicites)",
    "Résultats",
    "Pistes de recherche",
    "Citations",
]


# === HELPER FUNCTIONS ===
def clean_filename(s: str) -> str:
    s = re.sub(r"[^\w\s-]", "", s)
    s = re.sub(r"\s+", "_", s.strip())
    return s[:80]


def extract_authors(authors_list):
    if not authors_list:
        return ""
    names = []
    for a in authors_list:
        given = a.get("given", "")
        family = a.get("family", "")
        full = f"{family}, {given}".strip(", ")
        names.append(full)
    return "; ".join(names)


def extract_tags(tags_list):
    if not tags_list:
        return []
    extracted = []
    for t in tags_list:
        tag_value = t.get("tag") if isinstance(t, dict) else str(t)
        if tag_value:
            extracted.append(tag_value.strip())
    return extracted


def get_category(item_type):
    return TYPE_FOLDERS.get(item_type, "other")


def generate_filename(year, first_author, title):
    first_author = first_author.split(",")[0] if first_author else "Unknown"
    short_title = clean_filename(title.split(":")[0].split(".")[0])
    return f"{first_author}_{short_title}_{year}.md"


def normalize_item(item):
    meta = {
        "id": item.get("id"),
        "type": item.get("type"),
        "title": item.get("title", "").strip(),
        "authors": extract_authors(item.get("author", [])),
        "issued": item.get("issued", {}).get("date-parts", [[None]])[0][0],
        "container_title": item.get("container-title"),
        "DOI": item.get("DOI"),
        "URL": item.get("URL"),
        "source": item.get("source"),
        "tags": extract_tags(item.get("tags", [])),
        "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    return meta


def load_frontmatter(md_path):
    """Read YAML + body content from markdown."""
    with open(md_path, "r", encoding="utf-8") as f:
        content = f.read()

    if content.startswith("---"):
        parts = content.split("---", 2)
        if len(parts) >= 3:
            yaml_block = parts[1]
            body = parts[2].strip()
            try:
                metadata = yaml.safe_load(yaml_block)
                return metadata or {}, body
            except yaml.YAMLError:
                print(f"⚠️ Could not parse YAML in {md_path}")
    return {}, content


def write_frontmatter(md_path, metadata, body):
    yaml_block = yaml.dump(metadata, sort_keys=False, allow_unicode=True)
    with open(md_path, "w", encoding="utf-8") as f:
        f.write(f"---\n{yaml_block}---\n\n{body.strip()}\n")


def build_body_template():
    """Structured template for systematic review notes (in French)."""
    fields = [f"## {f}\n\n" for f in CUSTOM_FIELDS]
    return "\n".join(fields)


def parse_custom_fields(body):
    """
    Extract the text content following each ## FieldName section in the markdown body.
    Returns a dict of {FieldName: user_input}.
    """
    field_data = {f: "" for f in CUSTOM_FIELDS}
    pattern = re.compile(r"^## (.+?)\n([\s\S]*?)(?=\n## |\Z)", re.MULTILINE)
    for match in pattern.finditer(body):
        field, content = match.groups()
        field = field.strip()
        if field in field_data:
            field_data[field] = content.strip()
    return field_data


# === MAIN SCRIPT ===
def main():
    print("📚 Building literature review system...")
    NOTES_DIR.mkdir(exist_ok=True)
    INDEX_DIR.mkdir(exist_ok=True)

    if not ZOTERO_JSON.exists():
        print(f"❌ Zotero export not found: {ZOTERO_JSON}")
        return

    with open(ZOTERO_JSON, "r", encoding="utf-8") as f:
        zotero_data = json.load(f)

    all_records = []

    for item in zotero_data:
        meta = normalize_item(item)
        category = get_category(meta["type"])
        folder = NOTES_DIR / category
        folder.mkdir(exist_ok=True)

        filename = generate_filename(meta["issued"], meta["authors"].split(";")[0], meta["title"])
        filepath = folder / filename

        meta["category"] = category
        meta["path"] = str(filepath)

        if filepath.exists():
            existing_meta, body = load_frontmatter(filepath)
            changed = False

            # Update Zotero metadata if needed
            for key, value in meta.items():
                if existing_meta.get(key) != value:
                    existing_meta[key] = value
                    changed = True

            # Extract your manual note fields
            custom_data = parse_custom_fields(body)
            record = {**existing_meta, **custom_data}

            if changed:
                write_frontmatter(filepath, existing_meta, body)
                print(f"🌀 Updated metadata: {filepath.name}")
            else:
                print(f"↪️ No change: {filepath.name}")

        else:
            # Create a new markdown file with the structured template
            body_template = build_body_template()
            write_frontmatter(filepath, meta, body_template)
            print(f"✅ Created new note: {filepath.name}")
            record = {**meta, **{f: "" for f in CUSTOM_FIELDS}}

        all_records.append(record)

    # Convert to DataFrame and export
    df = pd.DataFrame(all_records)
    df.to_csv(CSV_INDEX, index=False)
    print(f"\n📊 Index updated: {CSV_INDEX}")
    print(f"🗂 Total records: {len(df)}")


if __name__ == "__main__":
    main()
