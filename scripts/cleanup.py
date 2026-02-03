"""
Developer utility to clean generated CSV artifacts from the pipeline.
Equivalent to `make clean` for data outputs.
"""

from config.settings import AppConfig


# ---- Cleanup Logic ----
def clean_project_data(dry_run: bool = False) -> None:
    """
    Remove generated CSV files from all pipeline stages.

    Args:
        dry_run: If True, prints what would be deleted without deleting.
    """
    config = AppConfig()

    files_to_remove = [
        config.raw_gs_path,
        config.norm_bp_path,
        config.norm_hr_path,
        config.norm_sleep_path,
        config.val_bp_path,
        config.val_hr_path,
        config.val_sleep_path,
        config.merged_path,
    ]

    deleted = 0

    for path in files_to_remove:
        if path.exists():
            if dry_run:
                print(f"[DRY RUN] Would delete: {path}")
            else:
                path.unlink()
                print(f"Deleted: {path}")
                deleted += 1

    print(f"\nCleanup complete. Files removed: {deleted}")


# ---- Script Entry Point ----
if __name__ == "__main__":
    clean_project_data()
