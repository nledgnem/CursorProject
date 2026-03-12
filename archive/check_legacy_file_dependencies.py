"""Check which scripts depend on legacy wide format files."""

from pathlib import Path
import re

def check_file_dependencies():
    """Check which Python files reference the legacy wide format files."""
    
    legacy_files = [
        'prices_daily.parquet',
        'marketcap_daily.parquet',
        'volume_daily.parquet'
    ]
    
    repo_root = Path(__file__).parent
    
    # Files to check
    files_to_check = list(repo_root.rglob('*.py'))
    
    dependencies = {}
    
    for py_file in files_to_check:
        if 'venv' in str(py_file) or '__pycache__' in str(py_file):
            continue
        
        try:
            content = py_file.read_text()
            file_deps = []
            
            for legacy_file in legacy_files:
                # Check for references to the file
                patterns = [
                    rf'"{legacy_file}"',  # String literal
                    rf"'{legacy_file}'",  # String literal
                    rf'{legacy_file}',     # Variable reference
                ]
                
                for pattern in patterns:
                    if re.search(pattern, content):
                        file_deps.append(legacy_file)
                        break
            
            if file_deps:
                dependencies[str(py_file.relative_to(repo_root))] = file_deps
        except Exception as e:
            pass
    
    return dependencies

def main():
    print("=" * 80)
    print("Legacy File Dependencies Analysis")
    print("=" * 80)
    print()
    
    dependencies = check_file_dependencies()
    
    print("Scripts that reference legacy wide format files:")
    print()
    
    critical_scripts = []
    other_scripts = []
    
    for file_path, deps in sorted(dependencies.items()):
        if 'scripts' in file_path:
            critical_scripts.append((file_path, deps))
        else:
            other_scripts.append((file_path, deps))
    
    print("CRITICAL SCRIPTS (in scripts/ directory):")
    print("-" * 80)
    for file_path, deps in critical_scripts:
        print(f"\n{file_path}:")
        print(f"  Uses: {', '.join(deps)}")
        print(f"  Impact: HIGH - Script would fail if files deleted")
    
    if other_scripts:
        print("\n" + "=" * 80)
        print("OTHER FILES:")
        print("-" * 80)
        for file_path, deps in other_scripts:
            print(f"\n{file_path}:")
            print(f"  Uses: {', '.join(deps)}")
    
    print("\n" + "=" * 80)
    print("Summary")
    print("=" * 80)
    print(f"\nTotal files referencing legacy format: {len(dependencies)}")
    print(f"Critical scripts: {len(critical_scripts)}")
    print(f"Other files: {len(other_scripts)}")
    
    if critical_scripts:
        print("\n⚠️  WARNING: Legacy files are still REQUIRED by critical scripts!")
        print("   You MUST refactor these scripts before deleting legacy files:")
        for file_path, deps in critical_scripts:
            print(f"   - {file_path}")
    else:
        print("\n✅ No critical scripts depend on legacy files!")
        print("   Safe to delete (but check other files first)")

if __name__ == '__main__':
    main()

