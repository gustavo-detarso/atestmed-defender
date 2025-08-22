# check_openai.py
import os, json
from dotenv import load_dotenv
import requests

def check_openai_from_env(env_path=".env", model="gpt-4o-mini"):
    load_dotenv(env_path)
    key = os.getenv("OPENAI_API_KEY")
    if not key:
        return False, "OPENAI_API_KEY não encontrado no .env"

    url = "https://api.openai.com/v1/chat/completions"
    headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}
    payload = {
        "model": model,
        "messages": [{"role": "user", "content": "ping"}],
        "max_tokens": 5,
    }
    r = requests.post(url, headers=headers, data=json.dumps(payload), timeout=20)
    try:
        data = r.json()
    except Exception:
        return False, f"HTTP {r.status_code}: {r.text[:200]}"

    if r.status_code == 200:
        return True, "OK — chave e créditos aparentam válidos."
    else:
        err = data.get("error", {})
        code = err.get("code")
        msg  = err.get("message", str(data))
        if r.status_code == 401:
            hint = "Chave inválida ou ausente."
        elif r.status_code == 429 and (code in ("insufficient_quota","rate_limit_exceeded","requests_limit_reached")):
            hint = "Créditos/limites esgotados."
        else:
            hint = "Falha na chamada."
        return False, f"{hint} (HTTP {r.status_code}, code={code}) {msg}"

if __name__ == "__main__":
    ok, info = check_openai_from_env()
    print(("✅ " if ok else "❌ ") + info)

