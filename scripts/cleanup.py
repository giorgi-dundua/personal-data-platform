"""
Developer utility to clean generated CSV artifacts from the pipeline.
"""
from config.settings import AppConfig


def clean_project_data(dry_run: bool = False, include_raw: bool = False) -> None:
    """
    Remove generated CSV files with optional raw file protection and error handling.
    """
    config = AppConfig()

    # 1. Define Processed Artifacts (Always targeted)
    artifacts = [
        config.norm_bp_path, config.norm_hr_path, config.norm_sleep_path,
        config.val_bp_path, config.val_hr_path, config.val_sleep_path,
        config.merged_path,
    ]

    # 2. Define Raw Directories (Only targeted if include_raw is True)
    raw_dirs = [config.RAW_MI_BAND_DATA_DIR, config.RAW_GOOGLE_SHEETS_DATA_DIR]

    targets = [(p, "Artifact") for p in artifacts]

    if include_raw:
        for folder in raw_dirs:
            if folder.exists():
                targets.extend([(f, "Raw File") for f in folder.glob("*.csv")])
    else:
        print("ℹ️  Skipping raw files (protection enabled). Use --raw to delete them.")

    deleted_count = 0

    for path, label in targets:
        if not path.exists():
            print(f"➖ Skipped: {path.name} (Not found)")
            continue

        if dry_run:
            print(f"🔍 [DRY RUN] Would delete {label}: {path.name}")
            deleted_count += 1
            continue

        try:
            path.unlink()
            print(f"✅ Deleted {label}: {path.name}")
            deleted_count += 1
        except PermissionError:
            print(f"❌ Failed: {path.name} (File is currently open in Excel or another app)")
        except Exception as e:
            print(f"❌ Error deleting {path.name}: {e}")

    status = "Identified" if dry_run else "Removed"
    print(f"\n✨ Cleanup finished. Total files {status}: {deleted_count}")


if __name__ == "__main__":
    # If run directly as a script, default to a safe dry run
    clean_project_data(dry_run=True, include_raw=True)
