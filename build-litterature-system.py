#!/usr/bin/env python3
"""
build_lit_system.py

Single script to:
- Read Zotero JSON export (data/biblio.json)
- Create/update Markdown notes in notes/{books,articles,conferences}
- Update YAML frontmatter (metadata) without touching manual note bodies
- Parse article/custom-fields into index/literature_index.csv
- Parse books (overview/key concepts/chapters) into index/books_index.csv
- Detect added book chapters and create corresponding chapter_N columns
- Print a summary of processed items
"""

import json
import re
from pathlib import Path
from datetime import datetime

import yaml
import pandas as pd

# === CONFIGURATION ===
DATA_DIR = Path("data")
NOTES_DIR = Path("notes")
INDEX_DIR = Path("index")

ZOTERO_JSON = DATA_DIR / "biblio.json"
CSV_INDEX_LIT = INDEX_DIR / "literature_index.csv"
CSV_INDEX_BOOKS = INDEX_DIR / "books_index.csv"

TYPE_FOLDERS = {
    "book": "books",
    "article-journal": "articles",
    "paper-conference": "conferences",
}

# Your qualitative analysis fields for articles/conferences (French)
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
]

# Book-specific sections to map to CSV columns (trimmed template)
BOOK_SECTIONS = [
    ("overview", "# 🧭 Overview"),  # main_thesis etc found inside
    ("key_concepts", "# 🗝️ Key Concepts"),
    ("theoretical_contribution", "# 🧩 Theoretical Contributions"),
    ("intellectual_lineage", "# 🧠 Intellectual Lineage"),
    ("implications", "# 📈 Implications / Open Questions"),
]
# We will also detect "## Chapter X" headings and export them fully as chapter_1, chapter_2, ...


# === HELPERS ===
def clean_filename(s: str) -> str:
    if not s:
        return "untitled"
    s = re.sub(r"[^\w\s-]", "", s)
    s = re.sub(r"\s+", "_", s.strip())
    return s[:100]


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


def normalize_item(item):
    """Get basic metadata dict from Zotero item."""
    year = None
    issued = item.get("issued", {})
    if isinstance(issued, dict):
        date_parts = issued.get("date-parts", [[None]])
        try:
            year = date_parts[0][0]
        except Exception:
            year = None
    meta = {
        "id": item.get("id"),
        "type": item.get("type"),
        "title": (item.get("title") or "").strip(),
        "authors": extract_authors(item.get("author", [])),
        "issued": year,
        "container_title": item.get("container-title"),
        "DOI": item.get("DOI"),
        "URL": item.get("URL"),
        "source": item.get("source"),
        # tags: read from YAML frontmatter later (optional)
        "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    return meta


# YAML frontmatter helpers
def load_frontmatter(md_path: Path):
    """
    Return (metadata_dict, body_text).
    If no frontmatter present, return ({}, full_content).
    """
    content = md_path.read_text(encoding="utf-8")
    if content.startswith("---"):
        parts = content.split("---", 2)
        if len(parts) >= 3:
            yaml_block = parts[1]
            body = parts[2].lstrip("\n")
            try:
                metadata = yaml.safe_load(yaml_block) or {}
            except Exception:
                metadata = {}
            return metadata, body
    return {}, content


def write_frontmatter(md_path: Path, metadata: dict, body: str):
    # Dump YAML with safe formatting
    yaml_block = yaml.safe_dump(metadata, sort_keys=False, allow_unicode=True).rstrip() + "\n"
    md_path.write_text(f"---\n{yaml_block}---\n\n{body.strip()}\n", encoding="utf-8")


# Article/conference body template generator
def build_article_body_template():
    lines = []
    for f in CUSTOM_FIELDS:
        lines.append(f"# {f}\n\n")
    return "\n".join(lines)


# Book body template generator (trimmed + single Chapter 1)
def build_book_body_template():
    parts = []
    # Add the trimmed sections
    parts.append("# 🧭 Overview\n- **Main thesis or argument:**\n- **Theoretical framework:**\n- **Disciplinary / historical context:**\n")
    parts.append("# 🗝️ Key Concepts\n- \n")
    parts.append("# 🧩 Theoretical Contributions\n- \n")
    parts.append("# 🧠 Intellectual Lineage\n- \n")
    parts.append("# 📈 Implications / Open Questions\n- \n")
    parts.append("# 📚 Chapter 1\n- \n")
    return "\n".join(parts)


# Parsing functions for article custom fields (extract full text under each "## Field" heading)
def parse_custom_fields(body_text: str):
    """
    Return dict {FieldName: content_text} for CUSTOM_FIELDS.
    If a field is missing, empty string.
    """
    data = {f: "" for f in CUSTOM_FIELDS}
    # Pattern captures heading line "## Field" and any content until next "## " or end
    pattern = re.compile(r"^##\s+(.+?)\s*\n([\s\S]*?)(?=(?:\n##\s+)|\Z)", re.MULTILINE)
    for m in pattern.finditer(body_text):
        heading = m.group(1).strip()
        content = m.group(2).strip()
        if heading in data:
            data[heading] = content
    return data


# Parsing functions for books:
def parse_book_sections_and_chapters(body_text: str):
    """
    Return a dict with keys:
      - overview, key_concepts, theoretical_contribution, intellectual_lineage, implications
      - chapter_1, chapter_2, ... (full text under each '# Chapter N' heading)
    """
    result = {
        "overview": "",
        "key_concepts": "",
        "theoretical_contribution": "",
        "intellectual_lineage": "",
        "implications": "",
    }

    # First extract the named top-level sections defined in BOOK_SECTIONS by their titles.
    # We'll search for those exact titles (as in the template) as top-level headings (starting with "# ")
    for key, marker in BOOK_SECTIONS:
        # escape marker for regex; match the marker line and capture until next top-level heading starting with "# "
        pattern = re.compile(re.escape(marker) + r"\s*\n([\s\S]*?)(?=(?:\n#\s)|\Z)", re.IGNORECASE)
        m = pattern.search(body_text)
        if m:
            extracted = m.group(1).strip()
            result[key] = extracted

    # Now capture all "# Chapter X" headings and their full text (until next "## " or next top-level "# " or end)
    chapter_pattern = re.compile(r"^#\s*Chapter\s*(\d+)\s*\n([\s\S]*?)(?=(?:\n##\s*Chapter\s*\d+\s*\n)|(?:\n#\s)|\Z)", re.MULTILINE | re.IGNORECASE)
    chapters = {}
    for m in chapter_pattern.finditer(body_text):
        num = int(m.group(1))
        text = m.group(2).strip()
        chapters[f"chapter_{num}"] = text

    # Ensure chapter_1 exists (even if empty) since template includes it
    if "chapter_1" not in chapters:
        chapters["chapter_1"] = ""

    # merge chapters into result
    result.update(chapters)
    return result


# Helper to safely get first author family name
def first_author_family(authors_str: str):
    if not authors_str:
        return "Unknown"
    first = authors_str.split(";")[0]
    family = first.split(",")[0].strip()
    return family if family else "Unknown"


def generate_filename(year, authors, title):
    y = str(year) if year else "YYYY"
    author = first_author_family(authors)
    short_title = clean_filename((title or "untitled").split(":")[0].split(".")[0])
    return f"{author}_{short_title}_{y}.md"


# Extract YAML tags (if present) and ensure list
def ensure_tags_from_metadata(metadata):
    tags = metadata.get("tags", [])
    if isinstance(tags, str):
        # maybe comma separated
        return [t.strip() for t in tags.split(",") if t.strip()]
    if not tags:
        return []
    if isinstance(tags, list):
        # tags might be list of dicts or strings
        out = []
        for t in tags:
            if isinstance(t, dict):
                out.append(t.get("tag", "").strip())
            else:
                out.append(str(t).strip())
        return [x for x in out if x]
    return []


# === MAIN PROCESSING ===
def main():
    print("📚 Starting build_lit_system...")

    NOTES_DIR.mkdir(exist_ok=True)
    INDEX_DIR.mkdir(exist_ok=True)
    # create subfolders
    for sub in set(TYPE_FOLDERS.values()):
        (NOTES_DIR / sub).mkdir(parents=True, exist_ok=True)

    if not ZOTERO_JSON.exists():
        print(f"❌ Zotero JSON not found at {ZOTERO_JSON}. Place your export there and re-run.")
        return

    with open(ZOTERO_JSON, "r", encoding="utf-8") as fh:
        try:
            zotero_items = json.load(fh)
        except Exception as e:
            print(f"❌ Failed to load JSON: {e}")
            return

    lit_records = []   # for articles + conferences
    book_records = []  # for books
    counts = {"articles": 0, "conferences": 0, "books": 0}

    # We'll collect all chapter keys across books to ensure consistent CSV columns later
    all_book_chapter_keys = set()

    for item in zotero_items:
        meta = normalize_item(item)
        item_type = meta.get("type")
        folder_name = TYPE_FOLDERS.get(item_type, "other")
        folder_path = NOTES_DIR / folder_name
        folder_path.mkdir(parents=True, exist_ok=True)

        # --- Robust filename / rename logic ---
        filename = generate_filename(meta.get("issued"), meta.get("authors"), meta.get("title"))
        filepath = folder_path / filename

        # Try to find an existing file that matches this Zotero ID
        existing_file = None
        for p in folder_path.glob("*.md"):
            try:
                fm, _ = load_frontmatter(p)
                if fm.get("id") and meta.get("id") and str(fm.get("id")) == str(meta.get("id")):
                    existing_file = p
                    break
            except Exception:
                continue

        if existing_file:
            # If filename has changed (due to updated title/author/year), rename safely
            if existing_file.resolve() != filepath.resolve():
                if filepath.exists():
                    print(f"⚠️ Target filename already exists: {filepath.name}. Keeping {existing_file.name}")
                else:
                    existing_file.rename(filepath)
                    print(f"🔄 Renamed file (metadata change): {existing_file.name} → {filepath.name}")

                # Update YAML path after rename
                existing_meta, body = load_frontmatter(filepath)
                existing_meta["path"] = str(filepath)
                write_frontmatter(filepath, existing_meta, body)

            # Use this file going forward
            filepath = filepath if filepath.exists() else existing_file

        else:
            # No existing file with same ID → create new one
            author = first_author_family(meta.get("authors"))
            short_title = clean_filename((meta.get("title") or "untitled").split(":")[0].split(".")[0])
            # Handle legacy "YYYY_" case if applicable
            old_pattern = folder_path / f"{author}_{short_title}_YYYY.md"
            if not filepath.exists() and old_pattern.exists():
                old_pattern.rename(filepath)
                print(f"🔄 Renamed file (added year): {old_pattern.name} → {filepath.name}")
            else:
                # Create a new note
                new_meta = dict(meta)
                if "tags" not in new_meta:
                    new_meta["tags"] = []
                if item_type == "book":
                    body = build_book_body_template()
                else:
                    body = build_article_body_template()
                write_frontmatter(filepath, new_meta, body)
                meta = new_meta
                import os
                try:
                    rel_path = os.path.relpath(filepath, Path.cwd())
                except ValueError:
                    rel_path = str(filepath)
                print(f"✅ Created new note: {rel_path}")


        # Add category and path into metadata
        meta["category"] = folder_name
        meta["path"] = str(filepath)

        # If file exists, load existing frontmatter and body; else create file with template
        if filepath.exists():
            existing_meta, body = load_frontmatter(filepath)
            # Merge metadata: update Zotero fields into frontmatter (without removing user fields like tags)
            changed = False
            for k, v in meta.items():
                # For tags we want to keep existing tags if present; but also ensure meta includes tags key
                if k == "last_updated":
                    # always update last_updated to now
                    existing_meta[k] = v
                    changed = True
                    continue
                if existing_meta.get(k) != v:
                    existing_meta[k] = v
                    changed = True
            # Save back only if changed
            if changed:
                write_frontmatter(filepath, existing_meta, body)
                # refresh metadata variable to include YAML tags if any
                meta = existing_meta
            else:
                # keep using existing_meta to capture tags if present
                meta = existing_meta
        else:
            # New file creation
            new_meta = dict(meta)
            if "tags" not in new_meta:
                new_meta["tags"] = []
            if item_type == "book":
                body = build_book_body_template()
            else:
                body = build_article_body_template()
            write_frontmatter(filepath, new_meta, body)
            meta = new_meta
            import os
            try:
                rel_path = os.path.relpath(filepath, Path.cwd())
            except ValueError:
                rel_path = str(filepath)
            print(f"✅ Created new note: {rel_path}")


        # After ensuring file exists, parse appropriate content and append to records
        # Reload frontmatter and body to ensure we have freshest content (in case we just wrote)
        front_meta, body = load_frontmatter(filepath)
        # ensure tags normalized
        front_meta_tags = ensure_tags_from_metadata(front_meta)

        if item_type == "book":
            counts["books"] += 1
            # Parse book sections and chapters
            book_data = parse_book_sections_and_chapters(body)
            # Prepare record merging metadata and parsed fields
            record = {
                "id": front_meta.get("id"),
                "title": front_meta.get("title"),
                "authors": front_meta.get("authors"),
                "issued": front_meta.get("issued"),
                "DOI": front_meta.get("DOI"),
                "URL": front_meta.get("URL"),
                "source": front_meta.get("source"),
                "tags": "; ".join(front_meta_tags) if front_meta_tags else "",
                "category": front_meta.get("category"),
                "path": front_meta.get("path"),
                "last_updated": front_meta.get("last_updated"),
                # map book_data named sections
                "main_thesis": book_data.get("overview", ""),
                "key_concepts": book_data.get("key_concepts", ""),
                "theoretical_contribution": book_data.get("theoretical_contribution", ""),
                "intellectual_lineage": book_data.get("intellectual_lineage", ""),
                "implications": book_data.get("implications", ""),
            }
            # attach chapter_N entries and remember keys
            for k, v in book_data.items():
                if k.startswith("chapter_"):
                    record[k] = v
                    all_book_chapter_keys.add(k)
            book_records.append(record)
        else:
            # treat as article or conference -> literature_index
            if item_type == "article-journal":
                counts["articles"] += 1
            elif item_type == "paper-conference":
                counts["conferences"] += 1
            else:
                # other types treated as 'articles' bucket
                counts["articles"] += 1

            # parse custom fields from body (articles template)
            custom = parse_custom_fields(body)
            record = {
                "id": front_meta.get("id"),
                "title": front_meta.get("title"),
                "authors": front_meta.get("authors"),
                "issued": front_meta.get("issued"),
                "DOI": front_meta.get("DOI"),
                "URL": front_meta.get("URL"),
                "container_title": front_meta.get("container_title"),
                "source": front_meta.get("source"),
                "tags": "; ".join(front_meta_tags) if front_meta_tags else "",
                "category": front_meta.get("category"),
                "path": front_meta.get("path"),
                "last_updated": front_meta.get("last_updated"),
            }
            # add custom fields (French headings)
            for k in CUSTOM_FIELDS:
                record[k] = custom.get(k, "")
            lit_records.append(record)

    # === WRITE CSVs ===
    # 1) Literature CSV (articles + conferences)
    if lit_records:
        df_lit = pd.DataFrame(lit_records)
        # ensure consistent column order: important fields first, then custom fields
        cols = ["id", "title", "authors", "issued", "DOI", "URL", "container_title", "source", "tags", "category", "path", "last_updated"]
        cols += CUSTOM_FIELDS
        # keep only existing columns in df
        cols = [c for c in cols if c in df_lit.columns]
        # append any other columns found in df_lit
        other_cols = [c for c in df_lit.columns if c not in cols]
        df_lit = df_lit[cols + other_cols]
        df_lit.to_csv(CSV_INDEX_LIT, index=False)
    else:
        # create empty CSV if none
        pd.DataFrame(columns=["id", "title", "authors"]).to_csv(CSV_INDEX_LIT, index=False)

    # 2) Books CSV (with dynamic chapter columns)
    if book_records:
        # Determine ordered chapter columns based on all_book_chapter_keys
        # Create an ordered list chapter_1, chapter_2, ... up to max found
        chapter_nums = sorted(
            [int(k.split("_")[1]) for k in all_book_chapter_keys if k.startswith("chapter_")] or []
        )
        chapter_cols = [f"chapter_{n}" for n in chapter_nums]

        # base book columns
        base_cols = [
            "id", "title", "authors", "issued", "DOI", "URL", "source", "tags",
            "category", "path", "last_updated",
            "main_thesis", "key_concepts", "theoretical_contribution", "intellectual_lineage", "implications"
        ]
        # build DataFrame and ensure all chapter cols exist (fill missing with empty strings)
        df_books = pd.DataFrame(book_records)
        for c in chapter_cols:
            if c not in df_books.columns:
                df_books[c] = ""
        # arrange columns
        cols = [c for c in base_cols if c in df_books.columns] + chapter_cols + [c for c in df_books.columns if c not in (base_cols + chapter_cols)]
        df_books = df_books[cols]
        df_books.to_csv(CSV_INDEX_BOOKS, index=False)
    else:
        pd.DataFrame(columns=["id", "title", "authors"]).to_csv(CSV_INDEX_BOOKS, index=False)

    # === Progress summary ===
    total_articles = counts["articles"]
    total_confs = counts["conferences"]
    total_books = counts["books"]

    print("\n📚 Literature database updated")
    print(f"  • {total_articles} articles")
    print(f"  • {total_confs} conferences")
    print(f"  • {total_books} books")
    print(f"\nCSV saved to: {CSV_INDEX_LIT} (articles/conferences) and {CSV_INDEX_BOOKS} (books)")

    # optional: return dataframes for interactive use
    return


if __name__ == "__main__":
    main()
