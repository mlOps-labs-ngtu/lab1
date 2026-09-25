.PHONY: passwords

passwords:
	@python3 -c '\
import json, secrets; \
pwd = secrets.token_urlsafe(16); \
json.dump({"admin": pwd}, open("config/passwords.json", "w"), indent=2); \
print("Пароль для admin:", pwd)'
	@chmod 644 config/passwords.json
