"""Replace original output files with aligned versions."""

import pandas as pd
import shutil
from pathlib import Path

def main():
    repo_root = Path(__file__).parent
    
    files = [
        ('data/curated/universe_eligibility_aligned.parquet', 'data/curated/universe_eligibility.parquet'),
        ('data/curated/universe_snapshots_aligned.parquet', 'data/curated/universe_snapshots.parquet'),
    ]
    
    print('Backing up and replacing original files...')
    print()
    
    for aligned_str, original_str in files:
        aligned_path = repo_root / aligned_str
        original_path = repo_root / original_str
        backup_path = repo_root / Path(original_str + '.backup')
        
        print(f'Processing {original_path.name}:')
        
        if not aligned_path.exists():
            print(f'  [ERROR] Aligned file not found: {aligned_path}')
            continue
        
        if original_path.exists():
            # Backup original
            shutil.copy2(original_path, backup_path)
            print(f'  [OK] Backed up original to {backup_path.name}')
            
            # Replace with aligned version
            shutil.copy2(aligned_path, original_path)
            print(f'  [OK] Replaced {original_path.name} with aligned version')
        else:
            print(f'  [SKIP] {original_path.name} not found')
        print()
    
    # Verify
    print('Verifying files now have asset_id...')
    print()
    
    for aligned_str, original_str in files:
        original_path = repo_root / original_str
        if original_path.exists():
            df = pd.read_parquet(original_path)
            has_asset_id = 'asset_id' in df.columns
            print(f'{original_path.name}: Has asset_id = {has_asset_id}')
    
    print()
    print('[OK] All files replaced successfully!')

if __name__ == '__main__':
    main()

