from config.settings import config
print("--- SCOPE CHECK ---")
for scope in config.GOOGLE_API_SCOPES:
    print(f"SCOPE: {scope}")
