import os

with open("full_codebase.txt", "w", encoding="utf-8") as out:
    for root, dirs, files in os.walk("."):
        # Ignore the heavy folders that break file uploads
        dirs[:] = [d for d in dirs if d not in {".venv", "venv", "__pycache__", ".git", ".cursor", "evidence"}]
        for file in files:
            if file.endswith((".py", ".yaml")):
                filepath = os.path.join(root, file)
                out.write(f"\n\n{'='*60}\nFILE: {filepath}\n{'='*60}\n\n")
                try:
                    with open(filepath, "r", encoding="utf-8") as f:
                        out.write(f.read())
                except Exception as e:
                    out.write(f"<Error reading file: {e}>\n")
print("Done. Generated full_codebase.txt")
