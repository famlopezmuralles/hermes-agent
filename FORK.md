# This is a fork

**Upstream:** [NousResearch/hermes-agent](https://github.com/NousResearch/hermes-agent)

**This repo:** [famlopezmuralles/hermes-agent](https://github.com/famlopezmuralles/hermes-agent)

linux25 / user `hermes1` runs **this fork**, not Nous upstream.

| Remote | URL |
|---|---|
| `origin` | `https://github.com/famlopezmuralles/hermes-agent.git` |
| `upstream` | `https://github.com/NousResearch/hermes-agent.git` |

Checkout on the host: `/home/hermes1/.hermes/hermes-agent`

MoneyManagerPlus SMS ingest is a **bundled platform plugin** (`plugins/platforms/mmp_sms/`), not a patch to `webhook.py` / WhatsApp.

Enable with `platforms.mmp_sms.enabled: true` (listens on `extra.port`, default 8001). Python is transport/dedupe only; the LLM parses bank SMS.

Do **not** open those PRs against Nous. Agent persona/config is the private repo [famlopezmuralles/hermes1](https://github.com/famlopezmuralles/hermes1).
