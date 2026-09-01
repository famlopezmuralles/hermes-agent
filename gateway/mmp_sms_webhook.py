"""MoneyManagerPlus SMS capture flow for the Hermes webhook adapter.

Transport only on the Python side:
- accept the LAN SMS contract;
- dedupe by (sender, body, receivedAt);
- hand the raw SMS to the Hermes LLM (WhatsApp delivery).

The LLM parses bank formats, canonicalizes merchants, writes the journal,
and reports budget execution. Do not add bank-specific regex here.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
import subprocess
import threading
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)

_REQUIRED_FIELDS = {
    "type": "sms_transaction_notification",
    "user": "carlos",
    "source": "termux-sms",
}
_PENDING_TTL = timedelta(days=14)
_CONFIRM_RE = re.compile(r"^\s*(CONFIRMAR|CONFIRMO|RECHAZAR|RECHAZO)\s+([A-Za-z0-9_-]{4,32})\s*$", re.I)
_DATE_RE = re.compile(r"\b(\d{1,2})[/-](\d{1,2})(?:[/-](\d{2,4}))?\b")
_LAST_DIGITS_RE = re.compile(
    r"(?:tarjeta|card|tc|terminada\s+en|\*{2,}|xx)[^0-9]{0,8}(\d{4})\b",
    re.I,
)
_PURCHASE_RE = re.compile(r"\b(?:compra|consumo|cargo|transacci[oó]n)\b", re.I)
_IGNORE_RE = re.compile(
    r"(inicio de sesi[oó]n|banca virtual|"
    r"tu oferta|promoci[oó]n|encuesta|gana beneficios|"
    r"contact.?center|ponemos a su disposici[oó]n|"
    r"operaci[oó]n fallida|"
    r"cuota vence|"
    r"habilita tu wallet|lleva tu tarjeta|"
    r"compra hoy y paga|"
    r"obten \d+% de comisi[oó]n|"
    r"participa para ganar|"
    r"test firewall|texto completo del SMS)",
    re.I,
)
_INSTITUTION_HINTS = (
    (re.compile(r"FICOAVISO|\bFICOHSA\b", re.I), "FICOHSA"),
    (re.compile(r"BIMOVIL|BANCO INDUSTRIAL", re.I), "BANCO INDUSTRIAL"),
    (re.compile(r"BANCO GTC|\bGTC\b", re.I), "GTC"),
    (re.compile(r"BAMAVISA|PROMOSBAM|\bBAM\b", re.I), "BAM"),
    (re.compile(r"PROMERICA", re.I), "PROMERICA"),
    (re.compile(r"BANRURAL", re.I), "BANRURAL"),
)


class _JournalValidationError(RuntimeError):
    pass

# The mapping is deliberately limited to accounts already present in MMP
# journals. Unknown categories remain needs_review; they never become a made-up
# expenses:* account.
_CATEGORY_ACCOUNT = {
    "transporte": "expenses:variable:transporte",
    "super": "expenses:variable:super",
    "restaurantes": "expenses:variable:restaurante",
    "streaming": "expenses:variable:streaming",
    "farmacia": "expenses:variable:farmacia",
    "ropa": "expenses:variable:compras:ropa",
    "pan": "expenses:variable:pan",
    "salidas familia": "expenses:variable:entretenimiento",
    "carlos varios": "expenses:variable:carlos",
}


def _norm(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip()).upper()


def _iso_date(value: str) -> datetime:
    raw = str(value or "").strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    parsed = datetime.fromisoformat(raw)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _parse_amount(body: str) -> tuple[Optional[Decimal], str]:
    # Prefer an amount explicitly marked as currency. This avoids selecting a
    # balance/account number from ordinary bank SMS wording.
    patterns = [
        (r"(?:Q|GTQ)\.?\s*([0-9][0-9,]*(?:\.\d{1,2})?)", "Q"),
        (r"([0-9][0-9,]*(?:\.\d{1,2})?)\s*(?:Q|GTQ)\b", "Q"),
        (r"(?:US\.?D?|USD|\$)\s*([0-9][0-9,]*(?:\.\d{1,2})?)", "$"),
        (r"([0-9][0-9,]*(?:\.\d{1,2})?)\s*(?:US\.?D?|USD|\$)\b", "$"),
    ]
    for pattern, currency in patterns:
        match = re.search(pattern, body, re.I)
        if not match:
            continue
        try:
            return Decimal(match.group(1).replace(",", "")), currency
        except InvalidOperation:
            return None, currency
    return None, ""


def _parse_merchant(body: str, amount: Optional[Decimal]) -> str:
    text = re.sub(r"\s+", " ", body or "").strip(" .:-")
    # Common bank templates. FICOAVISO is "Transaccion TC xx0124 por Q X en MERCHANT".
    patterns = [
        r"(?:por|de)\s+(?:Q|GTQ|USD|US\.?D?|\$)\.?\s*[0-9][0-9,]*(?:\.\d{1,2})?\s+en\s+(.+?)(?:,|\s+si\s+no|\s+Cuenta|$)",
        r"(?:compra|consumo|cargo|transacci[oó]n)\s+(?:en\s+)?(.+?)(?:\s+(?:por|de)\s+(?:Q|GTQ|USD|\$)?\s*[0-9])",
        r"(?:en\s+)(.+?)(?:\s+(?:por|de)\s+(?:Q|GTQ|USD|\$)?\s*[0-9])",
        r"(?:comercio|merchant)\s*[:=-]\s*(.+?)(?:\s+(?:Q|GTQ|USD|\$)?\s*[0-9]|$)",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.I)
        if match:
            merchant = re.sub(r"\s+", " ", match.group(1)).strip(" .,:;-")
            if merchant:
                return merchant[:120]
    # Fallback: remove obvious amount/date/card tokens, but mark review if the
    # remaining body is not a credible merchant.
    cleaned = re.sub(r"(?:Q|GTQ|USD|\$)\s*[0-9][0-9,]*(?:\.\d{1,2})?", "", text, flags=re.I)
    cleaned = _DATE_RE.sub("", cleaned)
    cleaned = re.sub(r"\b(?:compra|consumo|cargo|transacci[oó]n|aprobada?|tarjeta|card)\b", "", cleaned, flags=re.I)
    return re.sub(r"\s+", " ", cleaned).strip(" .,:;-")[:120]


def _parse_dictionary(claude_path: Path) -> list[tuple[list[str], str]]:
    """Parse the live Merchant synonyms dictionary section from CLAUDE.md."""
    if not claude_path.exists():
        raise FileNotFoundError(f"MoneyManagerPlus CLAUDE.md not found: {claude_path}")
    lines = claude_path.read_text(encoding="utf-8").splitlines()
    start = next((i for i, line in enumerate(lines) if "Merchant synonyms dictionary" in line), None)
    if start is None:
        raise ValueError("Merchant synonyms dictionary section missing from CLAUDE.md")
    entries: list[tuple[list[str], str]] = []
    pending_patterns: list[str] = []
    in_block = False
    for line in lines[start + 1 :]:
        stripped = line.strip()
        if stripped == "```":
            if in_block:
                break
            in_block = True
            continue
        if not in_block or not stripped:
            continue
        if "→" in stripped:
            left, right = stripped.split("→", 1)
            patterns = pending_patterns + [p.strip() for p in left.split("/") if p.strip()]
            category = right.split("(", 1)[0].strip()
            entries.append((patterns, category))
            pending_patterns = []
        elif not stripped.startswith("#"):
            # The dictionary puts long merchant groups on continuation lines;
            # the final line carries the category arrow.
            pending_patterns.extend(p.strip() for p in stripped.split("/") if p.strip())
    return entries


def _canonicalize(merchant: str, entries: list[tuple[list[str], str]]) -> tuple[str, Optional[str]]:
    normalized = _norm(merchant)
    for patterns, category in entries:
        for pattern in patterns:
            if _norm(pattern) in normalized:
                canonical = pattern.title() if pattern.upper() != "PUMA" else "Puma"
                return canonical, category
    return merchant, None


_BUDGET_NAME_ALIASES = {
    "transporte": ("transporte", "gasolina"),
    "gasolina": ("transporte", "gasolina"),
    "super": ("super",),
    "pan": ("pan",),
    "restaurantes": ("restaurantes", "restaurante"),
    "farmacia": ("farmacia", "salud"),
    "casa": ("casa", "renta", "hipoteca"),
    "hipoteca": ("casa", "renta", "hipoteca"),
    "streaming": ("streaming",),
}


def _money_amount(value: Any) -> Optional[Decimal]:
    if isinstance(value, dict):
        value = value.get("amount")
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _pct(spent: Decimal, target: Decimal) -> Optional[int]:
    if target <= 0:
        return None
    return int((spent / target * 100).quantize(Decimal("1")))


def format_budget_status(line: dict[str, Any], *, category_label: str) -> str:
    """Render monthly (and weekly, if variable/pacing) execution for WhatsApp."""
    target = _money_amount(line.get("target"))
    actual = _money_amount(line.get("actual")) or Decimal("0")
    if target is None:
        return ""
    monthly_pct = _pct(actual, target)
    if monthly_pct is None:
        return ""
    budget_type = str(line.get("budgetTypeId") or "").lower()
    status = str(line.get("status") or "")
    over = status == "over" or monthly_pct >= 100
    name = str(line.get("name") or category_label)
    currency = ""
    if isinstance(line.get("target"), dict):
        currency = str(line["target"].get("currency") or "")
        if currency == "GTQ":
            currency = "Q"
    header = f"🚨 {name} EXCEDIDO" if over and monthly_pct >= 100 else f"📊 {name}"
    lines = [
        header,
        f"Mes: {monthly_pct}% ejecutado ({actual:.2f} / {target:.2f} {currency})".rstrip(),
    ]
    pacing = line.get("pacing") if isinstance(line.get("pacing"), dict) else None
    show_week = budget_type in {"variable", "var"} or pacing is not None
    if show_week and pacing:
        spent_week = _money_amount(pacing.get("spentThisPeriod")) or Decimal("0")
        periods = pacing.get("periods") or []
        idx = pacing.get("currentPeriodIndex")
        period = None
        if isinstance(idx, int):
            period = next((p for p in periods if p.get("index") == idx), None)
            if period is None and 0 <= idx < len(periods):
                period = periods[idx]
        allowance = _money_amount((period or {}).get("allowance"))
        if allowance is None:
            remaining = _money_amount(pacing.get("allowanceRemaining"))
            if remaining is not None:
                allowance = spent_week + remaining
        week_pct = _pct(spent_week, allowance) if allowance else None
        if week_pct is not None and allowance is not None:
            week_over = pacing.get("status") == "over" or week_pct >= 100
            if week_over:
                over = True
            lines.append(
                f"Semana: {week_pct}% ejecutado ({spent_week:.2f} / {allowance:.2f} {currency})".rstrip()
            )
    if over and monthly_pct < 100:
        lines[0] = f"🚨 {name} EXCEDIDO"
    return "\n".join(lines)


def _load_yaml(path: Path) -> dict:
    try:
        import yaml  # type: ignore
    except ImportError as exc:
        raise RuntimeError("PyYAML is required to read config/account_map.yaml") from exc
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    return data if isinstance(data, dict) else {}


class MmpSmsWebhookProcessor:
    """Pure-ish parser plus durable pending/confirmation operations."""

    def __init__(self, config: dict[str, Any]):
        self.config = config
        self.repo = Path(config.get("mmp_repo", "/tmp/mmp-combined")).expanduser().resolve()
        self.git_publish = bool(config.get("git_publish", False))
        self.git_remote = str(config.get("git_remote", "origin"))
        self.git_branch = str(config.get("git_branch") or "").strip()
        pending_path = Path(config["pending_path"]).expanduser() if config.get("pending_path") else None
        if pending_path is None:
            from hermes_constants import get_hermes_home
            pending_path = get_hermes_home() / "mmp_sms_pending.json"
        self.pending_path: Path = pending_path
        self.pending_path.parent.mkdir(parents=True, exist_ok=True)
        raw_ips = config.get("allowed_ips")
        if isinstance(raw_ips, str):
            try:
                raw_ips = json.loads(raw_ips)
            except json.JSONDecodeError:
                raw_ips = [part for part in raw_ips.split(",") if part.strip()]
        if not isinstance(raw_ips, list):
            raw_ips = ["127.0.0.1", "::1", "192.168.101.16"]
        self.allowed_ips = {str(x).strip() for x in raw_ips if str(x).strip()}
        self.chat_id = str(config.get("chat_id") or "").strip()
        self.budget_api_url = str(
            config.get("budget_api_url") or "http://192.168.101.16:3026"
        ).rstrip("/")
        self._lock = threading.RLock()

    def accepts_ip(self, remote: str) -> bool:
        return remote in self.allowed_ips

    def _read_pending(self) -> dict[str, dict]:
        try:
            data = json.loads(self.pending_path.read_text(encoding="utf-8"))
            return data if isinstance(data, dict) else {}
        except FileNotFoundError:
            return {}
        except Exception:
            logger.exception("Could not read MMP SMS pending store")
            return {}

    def _write_pending(self, data: dict[str, dict]) -> None:
        tmp = self.pending_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        tmp.replace(self.pending_path)

    def _prune(self, data: dict[str, dict], now: datetime) -> None:
        for key, item in list(data.items()):
            try:
                if now - _iso_date(item["created_at"]) > _PENDING_TTL:
                    data.pop(key, None)
            except Exception:
                data.pop(key, None)

    def ingest_raw(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Validate + dedupe only. No bank parsing — that is the LLM's job."""
        if not isinstance(payload, dict):
            raise ValueError("JSON payload must be an object")
        for key, expected in _REQUIRED_FIELDS.items():
            if payload.get(key) != expected:
                raise ValueError(f"Invalid {key}; expected {expected!r}")
        for key in ("sender", "body", "receivedAt"):
            if not isinstance(payload.get(key), str) or not payload[key].strip():
                raise ValueError(f"Missing or invalid {key}")
        body = payload["body"]
        fingerprint = hashlib.sha256(
            json.dumps(
                {"sender": payload["sender"], "body": body, "receivedAt": payload["receivedAt"]},
                sort_keys=True,
            ).encode()
        ).hexdigest()[:16]
        candidate_id = f"SMS-{fingerprint[:8]}"
        candidate = {
            "id": candidate_id,
            "fingerprint": fingerprint,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "received_at": payload["receivedAt"],
            "sender": payload["sender"],
            "body": body,
            "status": "queued_for_agent",
        }
        with self._lock:
            pending = self._read_pending()
            self._prune(pending, datetime.now(timezone.utc))
            for existing in pending.values():
                if existing.get("fingerprint") == fingerprint:
                    return existing
            pending[candidate_id] = candidate
            self._write_pending(pending)
        return candidate

    def mark_agent_dispatched(self, item_ids: list[str]) -> None:
        with self._lock:
            pending = self._read_pending()
            now = datetime.now(timezone.utc).isoformat()
            changed = False
            for item_id in item_ids:
                item = pending.get(item_id)
                if item and item.get("status") == "queued_for_agent":
                    item["status"] = "agent_dispatched"
                    item["dispatched_at"] = now
                    changed = True
            if changed:
                self._write_pending(pending)

    def llm_prompt(self, items: list[dict[str, Any]]) -> str:
        blocks = []
        for item in items:
            blocks.append(
                f"- id={item.get('id')} received={item.get('received_at')} from={item.get('sender')}\n"
                f"  {item.get('body')}"
            )
        listing = "\n".join(blocks)
        return (
            "SMS bancarios nuevos (Termux → MMPlus). TÚ parseas el texto. "
            "No uses ni asumas un parser Python de bancos.\n\n"
            f"Repo journal: {self.repo}\n"
            "Escribe en Y26/journal/<cuenta>.Y26.M<n>.journal del ciclo correcto "
            "(cutoff_day en config/account_map.yaml). "
            "Si git está en otra rama, igual escribe el posting y reporta el error de git.\n\n"
            "Reglas:\n"
            "1. Ignora OTP, login, promos, marketing, débitos/créditos entre cuentas propias sin comercio.\n"
            "2. Compras/consumos: banco, últimos 4, monto, moneda, comercio, fecha.\n"
            "   GTC: 'Consumo tarjeta credito con la cuenta NNNN … Monto: Q. X Localidad: MERCHANT'\n"
            "   Promerica: 'Consumo PROMERICA **NNNN Monto … Comercio …'\n"
            "   Ficohsa: 'FICOAVISO: Transaccion TC xxNNNN por Q X en MERCHANT'\n"
            "   BI: 'BiMovil: Consumo por Q.X en MERCHANT Cuenta TCREDITO'\n"
            "3. Canonicaliza comercio con CLAUDE.md (Merchant synonyms). "
            "Si no está, needs_review — no inventes categoría.\n"
            "4. Compras claras: escribe el posting YA. No pidas CONFIRMAR uno por uno.\n"
            "5. Dudosas: un solo WhatsApp con el lote.\n"
            "6. Tras escribir: % ejecutado del presupuesto del mes; si es variable "
            "(gasolina/transporte/super) también % de la semana. Alerta si excedido.\n"
            f"   API: {self.budget_api_url}/api/budget/month/YYYY-MM?currencyMode=GTQ\n"
            "7. No dupliques si el asiento ya existe.\n"
            "8. Responde en WhatsApp el resumen. Si TODO el lote es ignore, no hace falta mensaje.\n\n"
            f"SMS ({len(items)}):\n{listing}\n"
        )

    def prepare(self, payload: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(payload, dict):
            raise ValueError("JSON payload must be an object")
        for key, expected in _REQUIRED_FIELDS.items():
            if payload.get(key) != expected:
                raise ValueError(f"Invalid {key}; expected {expected!r}")
        for key in ("sender", "body", "receivedAt"):
            if not isinstance(payload.get(key), str) or not payload[key].strip():
                raise ValueError(f"Missing or invalid {key}")
        received = _iso_date(payload["receivedAt"])
        body = payload["body"]
        amount, currency = _parse_amount(body)
        merchant = _parse_merchant(body, amount)
        claude_path = self.repo / "CLAUDE.md"
        dictionary = _parse_dictionary(claude_path)
        canonical, category = _canonicalize(merchant, dictionary)
        last_digits_match = _LAST_DIGITS_RE.search(body)
        last_digits = last_digits_match.group(1) if last_digits_match else ""
        body_date = None
        date_match = _DATE_RE.search(body)
        if date_match:
            day, month, year = date_match.groups()
            year_num = int(year) if year else received.year
            if year_num < 100:
                year_num += 2000
            try:
                body_date = datetime(year_num, int(month), int(day), tzinfo=timezone.utc).date().isoformat()
            except ValueError:
                body_date = None
        tx_date = body_date or received.date().isoformat()
        fingerprint = hashlib.sha256(
            json.dumps({"sender": payload["sender"], "body": body, "receivedAt": payload["receivedAt"]}, sort_keys=True).encode()
        ).hexdigest()[:16]
        candidate_id = f"SMS-{fingerprint[:8]}"
        account = self._resolve_account(payload["sender"], last_digits, body)
        is_purchase = bool(_PURCHASE_RE.search(body))
        needs_review = amount is None or not merchant or category is None or account is None
        if _IGNORE_RE.search(body) or not is_purchase:
            disposition = "ignore"
        elif needs_review:
            disposition = "review"
        else:
            disposition = "auto"
        candidate = {
            "id": candidate_id,
            "fingerprint": fingerprint,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "received_at": payload["receivedAt"],
            "transaction_date": tx_date,
            "sender": payload["sender"],
            "body": body,
            "amount": str(amount) if amount is not None else None,
            "currency": currency,
            "merchant": merchant,
            "merchant_canonical": canonical,
            "category": category,
            "account": account,
            "last_digits": last_digits,
            "needs_review": needs_review,
            "disposition": disposition,
            "status": "pending",
        }
        with self._lock:
            pending = self._read_pending()
            self._prune(pending, datetime.now(timezone.utc))
            for existing in pending.values():
                if existing.get("fingerprint") == fingerprint:
                    return existing
            pending[candidate_id] = candidate
            self._write_pending(pending)
        return candidate

    def _resolve_account(self, sender: str, last_digits: str, body: str = "") -> Optional[str]:
        account_map_path = self.repo / "config" / "account_map.yaml"
        try:
            accounts = _load_yaml(account_map_path).get("accounts", {})
        except Exception as exc:
            logger.warning("Could not load MMP account map: %s", exc)
            return None
        matches = []
        sender_norm = _norm(sender)
        haystack = f"{sender} {body}"
        hinted = {
            institution
            for pattern, institution in _INSTITUTION_HINTS
            if pattern.search(haystack)
        }
        for name, info in accounts.items():
            if not isinstance(info, dict) or info.get("account_type") != "credit_card":
                continue
            institution = _norm(info.get("institution"))
            sender_match = sender_norm in institution or institution in sender_norm
            hint_match = institution in hinted
            if not sender_match and not hint_match:
                continue
            mask = str(info.get("card_mask") or "")
            if last_digits and mask and mask[-4:] == last_digits:
                return name
            if last_digits:
                journal_hits = False
                for journal in (self.repo / "Y26" / "journal").glob(f"{name}.Y26.M*.journal"):
                    text = journal.read_text(encoding="utf-8", errors="replace")
                    if re.search(rf"(?:cc\s*:\s*|TARJETA\s+){re.escape(last_digits)}\b", text, re.I):
                        journal_hits = True
                        break
                if journal_hits:
                    return name
            matches.append(name)
        return matches[0] if len(matches) == 1 else None

    def preview(self, candidate: dict[str, Any]) -> str:
        amount = f"{candidate['amount']} {candidate['currency']}" if candidate.get("amount") else "monto no detectado"
        category = candidate.get("category") or "needs_review (comercio sin mapeo)"
        account = candidate.get("account") or "tarjeta no identificada"
        review = "\n⚠ needs_review: falta dato verificable; no se escribirá hasta resolverlo." if candidate.get("needs_review") else ""
        return (
            f"📥 SMS bancario {candidate['id']}\n"
            f"Banco: {candidate['sender']}\n"
            f"Fecha: {candidate['transaction_date']}\n"
            f"Monto: {amount}\n"
            f"Comercio: {candidate.get('merchant_canonical') or candidate.get('merchant') or 'no detectado'}\n"
            f"Categoría propuesta: {category}\n"
            f"Tarjeta: {account}"
            f"{(' • últimos ' + candidate['last_digits']) if candidate.get('last_digits') else ''}\n"
            f"\nResponde exactamente: CONFIRMAR {candidate['id']} o RECHAZAR {candidate['id']}."
            f"{review}"
        )

    async def send_preview(self, candidate: dict[str, Any], runner: Any) -> None:
        try:
            from gateway.config import Platform
            adapter = runner.adapters.get(Platform.WHATSAPP)
            if adapter is None:
                raise RuntimeError("WhatsApp adapter is not connected")
            chat_id = self.chat_id
            if not chat_id:
                home = runner.config.get_home_channel(Platform.WHATSAPP)
                chat_id = home.chat_id if home else ""
            if not chat_id:
                raise RuntimeError("No WhatsApp confirmation chat configured")
            result = await adapter.send(chat_id, self.preview(candidate))
            if not result.success:
                raise RuntimeError(result.error or "WhatsApp send failed")
            logger.info("[mmp-sms] preview sent id=%s chat=%s", candidate["id"], chat_id)
        except Exception:
            logger.exception("[mmp-sms] preview delivery failed id=%s", candidate.get("id"))

    def apply_policy(self, candidate: dict[str, Any]) -> str:
        """Decide ignore / auto-write / WhatsApp preview. Returns the action taken."""
        if candidate.get("status") not in (None, "pending"):
            return "noop"
        disposition = candidate.get("disposition") or "review"
        if disposition == "ignore":
            with self._lock:
                pending = self._read_pending()
                stored = pending.get(candidate["id"])
                if stored and stored.get("status") == "pending":
                    stored["status"] = "ignored"
                    self._write_pending(pending)
            logger.info("[mmp-sms] ignored id=%s", candidate.get("id"))
            return "ignore"
        if disposition == "auto":
            notice = self.confirm("CONFIRMAR", candidate["id"])
            candidate["notice"] = notice
            logger.info("[mmp-sms] auto-confirmed id=%s result=%s", candidate.get("id"), notice)
            return "auto"
        return "preview"

    def auto_notice(self, candidate: dict[str, Any]) -> str:
        amount = f"{candidate.get('amount')} {candidate.get('currency')}".strip()
        merchant = candidate.get("merchant_canonical") or candidate.get("merchant") or "comercio"
        account = candidate.get("account") or "?"
        result = candidate.get("notice") or ""
        if "confirmado" in result.lower() or "escrito" in result.lower():
            text = (
                f"✅ Registré automáticamente {candidate['id']}: {merchant} {amount} "
                f"en {account}. Si no era, avísame."
            )
            return self._with_budget(candidate, text)
        return f"⚠ {candidate['id']}: no pude auto-registrar ({result})"

    def fetch_budget_month(self, year_month: str) -> Optional[dict[str, Any]]:
        url = f"{self.budget_api_url}/api/budget/month/{year_month}?currencyMode=GTQ"
        try:
            with urllib.request.urlopen(url, timeout=3) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, ValueError) as exc:
            logger.warning("[mmp-sms] budget API unavailable %s: %s", url, exc)
            return None
        return payload if isinstance(payload, dict) else None

    def match_budget_line(self, month: dict[str, Any], candidate: dict[str, Any]) -> Optional[dict[str, Any]]:
        lines = month.get("lines")
        if not isinstance(lines, list):
            return None
        needles = []
        for raw in (candidate.get("category"), candidate.get("merchant_canonical")):
            key = _norm(raw).lower()
            if key:
                needles.extend(_BUDGET_NAME_ALIASES.get(key, (key,)))
        expense = _CATEGORY_ACCOUNT.get(str(candidate.get("category") or "").lower(), "")
        if expense:
            needles.append(expense.rsplit(":", 1)[-1])
        needles = [_norm(n) for n in needles if n]
        for line in lines:
            if not isinstance(line, dict):
                continue
            hay = " ".join(
                str(line.get(k) or "")
                for k in ("name", "categoryId", "id")
            )
            hay_n = _norm(hay)
            if any(n and n in hay_n for n in needles):
                return line
        return None

    def budget_status(self, candidate: dict[str, Any]) -> str:
        tx_date = str(candidate.get("transaction_date") or "")
        year_month = tx_date[:7]
        if len(year_month) != 7:
            return ""
        month = self.fetch_budget_month(year_month)
        if not month:
            return ""
        line = self.match_budget_line(month, candidate)
        if not line:
            return ""
        label = str(candidate.get("category") or line.get("name") or "presupuesto")
        return format_budget_status(line, category_label=label)

    def _with_budget(self, candidate: dict[str, Any], text: str) -> str:
        extra = self.budget_status(candidate)
        return f"{text}\n{extra}" if extra else text

    async def send_notice(self, candidate: dict[str, Any], runner: Any) -> None:
        try:
            from gateway.config import Platform
            adapter = runner.adapters.get(Platform.WHATSAPP)
            if adapter is None:
                raise RuntimeError("WhatsApp adapter is not connected")
            chat_id = self.chat_id
            if not chat_id:
                home = runner.config.get_home_channel(Platform.WHATSAPP)
                chat_id = home.chat_id if home else ""
            if not chat_id:
                raise RuntimeError("No WhatsApp confirmation chat configured")
            result = await adapter.send(chat_id, self.auto_notice(candidate))
            if not result.success:
                raise RuntimeError(result.error or "WhatsApp send failed")
            logger.info("[mmp-sms] auto notice sent id=%s chat=%s", candidate["id"], chat_id)
        except Exception:
            logger.exception("[mmp-sms] auto notice failed id=%s", candidate.get("id"))

    def classify_control(self, text: str) -> Optional[tuple[str, str]]:
        match = _CONFIRM_RE.match(text or "")
        if not match:
            return None
        return match.group(1).upper(), match.group(2).upper()

    def confirm(self, action: str, candidate_id: str) -> str:
        with self._lock:
            pending = self._read_pending()
            candidate = pending.get(candidate_id)
            if candidate is None:
                candidate_key = next((key for key in pending if key.upper() == candidate_id.upper()), None)
                candidate = pending.get(candidate_key) if candidate_key else None
                if candidate_key:
                    candidate_id = candidate_key
            if not candidate:
                return f"No encuentro el SMS pendiente {candidate_id}."
            if action.startswith("RECHAZ"):
                candidate["status"] = "rejected"
                self._write_pending(pending)
                return f"SMS {candidate_id} rechazado. No se escribió journal."
            if candidate.get("status") != "pending":
                return f"SMS {candidate_id} ya está marcado como {candidate.get('status')}."
            if candidate.get("needs_review"):
                candidate["status"] = "needs_review"
                self._write_pending(pending)
                return f"SMS {candidate_id} requiere revisión. No escribo un asiento con datos ambiguos."
            journal_path = self._journal_path(candidate)
            if self.git_publish:
                try:
                    self._check_git_ready()
                except Exception as exc:
                    candidate["error"] = str(exc)
                    self._write_pending(pending)
                    return f"SMS {candidate_id}: no escribí; publicación bloqueada: {exc}"
            duplicate = self._find_duplicate(candidate)
            if duplicate:
                candidate["status"] = "duplicate"
                candidate["duplicate_of"] = str(duplicate)
                self._write_pending(pending)
                return f"SMS {candidate_id}: no escribí. Ya existe una operación coincidente en {duplicate}."
            posting = self._render_posting(candidate)
            journal_path.parent.mkdir(parents=True, exist_ok=True)
            original_size = journal_path.stat().st_size if journal_path.exists() else 0
            try:
                with journal_path.open("a", encoding="utf-8") as fh:
                    if journal_path.stat().st_size > 0 and not journal_path.read_text(encoding="utf-8").endswith("\n\n"):
                        fh.write("\n")
                    fh.write(posting)
                if self.git_publish:
                    self._validate_journal()
                    publish = self._publish_journal(candidate, journal_path)
                else:
                    publish = None
            except _JournalValidationError as exc:
                self._rollback_append(journal_path, original_size)
                candidate["status"] = "pending"
                self._write_pending(pending)
                return f"SMS {candidate_id}: no escribí; hledger rechazó el journal: {exc}"
            except Exception as exc:
                candidate["status"] = "publish_failed" if self.git_publish else "write_failed"
                candidate["error"] = str(exc)
                candidate["journal_path"] = str(journal_path)
                self._write_pending(pending)
                logger.exception("[mmp-sms] journal confirmation failed id=%s", candidate_id)
                return f"SMS {candidate_id}: journal local escrito, pero publicación falló: {exc}"
            candidate["status"] = "confirmed_published" if self.git_publish else "confirmed_written"
            candidate["journal_path"] = str(journal_path)
            if publish:
                candidate["git"] = publish
            self._write_pending(pending)
            logger.info("[mmp-sms] journal written id=%s path=%s", candidate_id, journal_path)
            if publish:
                return (
                    f"✅ SMS {candidate_id} confirmado, escrito y publicado: "
                    f"commit {publish['commit']}; push {publish['remote']}/{publish['branch']} verificado."
                )
            return f"✅ SMS {candidate_id} confirmado y escrito en {journal_path.name}."

    def _run_git(self, *args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
        result = subprocess.run(
            ["git", "-C", str(self.repo), *args],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=30,
        )
        if check and result.returncode != 0:
            detail = (result.stderr or result.stdout).strip()
            raise RuntimeError(f"git {' '.join(args)} falló: {detail}")
        return result

    def _check_git_ready(self, *, allow_dirty: bool = False) -> str:
        self._run_git("rev-parse", "--git-dir")
        branch = self._run_git("branch", "--show-current").stdout.strip()
        if not branch:
            raise RuntimeError("el repo está en detached HEAD; no publico automáticamente")
        if branch in {"main", "master"}:
            raise RuntimeError(f"push bloqueado en branch protegida {branch}")
        if self.git_branch and branch != self.git_branch:
            raise RuntimeError(f"branch inesperado: activo={branch}, requerido={self.git_branch}")
        dirty = self._run_git("status", "--porcelain", "--untracked-files=all").stdout.strip()
        if dirty and not allow_dirty:
            raise RuntimeError("el repo tiene cambios previos no relacionados; no los mezclo")
        return branch

    def _validate_journal(self) -> None:
        journal = self.repo / "Y26" / "Y26.journal"
        if not journal.exists():
            raise _JournalValidationError(f"no existe {journal}")
        try:
            result = subprocess.run(
                ["hledger", "--strict", "-f", str(journal), "check"],
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=60,
            )
        except FileNotFoundError as exc:
            raise _JournalValidationError("hledger no está instalado") from exc
        if result.returncode != 0:
            detail = (result.stderr or result.stdout).strip().replace("\n", " ")
            raise _JournalValidationError(detail or "check falló sin detalle")

    @staticmethod
    def _rollback_append(path: Path, original_size: int) -> None:
        if not path.exists():
            return
        with path.open("r+b") as fh:
            fh.truncate(original_size)
        if original_size == 0:
            path.unlink(missing_ok=True)

    def _publish_journal(self, candidate: dict[str, Any], journal_path: Path) -> dict[str, str]:
        branch = self._check_git_ready(allow_dirty=True)
        relative = journal_path.relative_to(self.repo).as_posix()
        status_lines = self._run_git("status", "--porcelain", "--untracked-files=all").stdout.splitlines()
        changed_paths = {line[3:] for line in status_lines if len(line) >= 4}
        if changed_paths != {relative}:
            raise RuntimeError(f"cambios inesperados después de escribir: {sorted(changed_paths)}")
        self._run_git("add", "--", relative)
        check = self._run_git("diff", "--cached", "--check", check=False)
        if check.returncode != 0:
            raise RuntimeError(f"git diff --check falló: {(check.stderr or check.stdout).strip()}")
        commit = self._run_git(
            "commit",
            "-m",
            f"feat(finance): record WhatsApp transaction {candidate['id']}",
        )
        sha = self._run_git("rev-parse", "HEAD").stdout.strip()
        self._run_git("push", self.git_remote, f"HEAD:{branch}")
        remote = self._run_git("ls-remote", self.git_remote, f"refs/heads/{branch}").stdout.split()
        if not remote or remote[0] != sha:
            raise RuntimeError(f"push no verificado: remoto={remote[0] if remote else 'ausente'}, local={sha}")
        return {"commit": sha[:12], "branch": branch, "remote": self.git_remote}

    def _journal_path(self, candidate: dict[str, Any]) -> Path:
        account = str(candidate["account"])
        account_map = _load_yaml(self.repo / "config" / "account_map.yaml").get("accounts", {})
        cutoff = int(account_map[account].get("cutoff_day", 1))
        tx = datetime.fromisoformat(candidate["transaction_date"]).date()
        month = tx.month if tx.day >= cutoff else (tx.month - 1 or 12)
        year = tx.year if tx.day >= cutoff else (tx.year if tx.month > 1 else tx.year - 1)
        if year != 2026:
            raise ValueError(f"Only Y26 journal routing is configured; got {year}")
        return self.repo / "Y26" / "journal" / f"{account}.Y26.M{month}.journal"

    def _find_duplicate(self, candidate: dict[str, Any]) -> Optional[Path]:
        amount = candidate.get("amount")
        currency = candidate.get("currency")
        if not amount or not currency:
            return None
        journal_dir = self.repo / "Y26" / "journal"
        account = candidate["account"]
        account_token = f"liabilities:tc:{account}:saldo"
        for path in sorted(journal_dir.glob(f"{account}.Y26.M*.journal")):
            text = path.read_text(encoding="utf-8", errors="replace")
            if candidate["transaction_date"] not in text:
                continue
            if account_token not in text:
                continue
            amount_token = f"-{Decimal(amount):.2f} {currency}"
            if amount_token not in text:
                continue
            merchant = _norm(candidate.get("merchant_canonical") or candidate.get("merchant"))
            if merchant and any(part in _norm(line) for line in text.splitlines() for part in [merchant] if part):
                return path
            if candidate.get("last_digits") and candidate["last_digits"] in text:
                return path
        return None

    @staticmethod
    def _render_posting(candidate: dict[str, Any]) -> str:
        account = candidate["account"]
        expense = _CATEGORY_ACCOUNT.get(str(candidate["category"]).lower())
        # PUMA is explicitly gasoline in the live dictionary and existing
        # journals, not generic transport.
        if str(candidate.get("merchant_canonical", "")).upper().startswith("PUMA"):
            expense = "expenses:variable:gasolina"
        if not expense:
            raise ValueError("No journal account mapping for canonical category")
        amount = Decimal(candidate["amount"])
        currency = candidate["currency"]
        meta = [f"source:mmp_sms", f"sms_id:{candidate['id']}", f"sender:{candidate['sender']}"]
        if candidate.get("last_digits"):
            meta.append(f"cc:****{candidate['last_digits']}")
        return (
            f"{candidate['transaction_date']} {candidate['merchant_canonical']}  ; "
            + " ".join(meta)
            + "\n"
            + f"    liabilities:tc:{account}:saldo    -{amount:.2f} {currency}\n"
            + f"    {expense}    {amount:.2f} {currency}\n"
        )


async def notify_confirmation_result(processor: MmpSmsWebhookProcessor, action: str, candidate_id: str, runner: Any, chat_id: str) -> None:
    result_text = await asyncio.to_thread(processor.confirm, action, candidate_id)
    pending = processor._read_pending()
    stored = pending.get(candidate_id) or next(
        (item for key, item in pending.items() if key.upper() == candidate_id.upper()),
        None,
    )
    if stored:
        result_text = processor._with_budget(stored, result_text)
    try:
        from gateway.config import Platform
        adapter = runner.adapters.get(Platform.WHATSAPP)
        if adapter is not None:
            await adapter.send(chat_id, result_text)
    except Exception:
        logger.exception("[mmp-sms] confirmation reply failed id=%s", candidate_id)
