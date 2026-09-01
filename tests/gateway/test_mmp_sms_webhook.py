import asyncio
from pathlib import Path
import subprocess
from types import SimpleNamespace

import aiohttp

from gateway.config import PlatformConfig
from gateway.mmp_sms_webhook import MmpSmsWebhookProcessor, format_budget_status
from gateway.platforms.webhook import WebhookAdapter


def _copy_repo(tmp_path: Path) -> Path:
    repo = tmp_path / "mmp"
    (repo / "config").mkdir(parents=True)
    (repo / "Y26" / "journal").mkdir(parents=True)
    source = Path("/home/hermes1/repos/MoneyManagerPlus")
    if not (source / "CLAUDE.md").exists():
        source = Path("/tmp/mmp-combined")
    (repo / "CLAUDE.md").write_text((source / "CLAUDE.md").read_text(encoding="utf-8"), encoding="utf-8")
    (repo / "config" / "account_map.yaml").write_text(
        (source / "config" / "account_map.yaml").read_text(encoding="utf-8"), encoding="utf-8"
    )
    (repo / "Y26" / "journal" / "mcblack.Y26.M8.journal").write_text("; test\n", encoding="utf-8")
    return repo


def _payload(body="Compra en PUMA ENERGY GT LA FRON por Q 300.00 tarjeta terminada en 2927"):
    return {
        "type": "sms_transaction_notification",
        "user": "carlos",
        "sender": "Promerica",
        "body": body,
        "receivedAt": "2026-08-26T18:00:00.000Z",
        "source": "termux-sms",
    }


def test_prepare_uses_live_dictionary_and_routes_cycle(tmp_path):
    repo = _copy_repo(tmp_path)
    processor = MmpSmsWebhookProcessor(
        {"mmp_repo": str(repo), "pending_path": str(tmp_path / "pending.json")}
    )
    candidate = processor.prepare(_payload())
    assert candidate["merchant_canonical"] == "Puma"
    assert candidate["category"] == "Transporte"
    assert candidate["account"] == "mcblack"
    assert candidate["needs_review"] is False
    assert processor._journal_path(candidate).name == "mcblack.Y26.M8.journal"


def test_unknown_or_ambiguous_sms_never_writes(tmp_path):
    repo = _copy_repo(tmp_path)
    processor = MmpSmsWebhookProcessor(
        {"mmp_repo": str(repo), "pending_path": str(tmp_path / "pending.json")}
    )
    candidate = processor.prepare(
        _payload("Compra en COMERCIO NUEVO por Q 125.00") | {"sender": "BAM"}
    )
    assert candidate["needs_review"] is True
    assert "requiere revisión" in processor.confirm("CONFIRMAR", candidate["id"])
    assert (repo / "Y26" / "journal" / "baminfinite.Y26.M8.journal").exists() is False


def test_confirmation_is_explicit_and_id_case_insensitive(tmp_path):
    repo = _copy_repo(tmp_path)
    processor = MmpSmsWebhookProcessor(
        {"mmp_repo": str(repo), "pending_path": str(tmp_path / "pending.json")}
    )
    candidate = processor.prepare(_payload())
    assert not (repo / "Y26" / "journal" / "mcblack.Y26.M8.journal").read_text(encoding="utf-8").endswith("300.00 Q\n")
    result = processor.confirm("CONFIRMAR", candidate["id"].upper())
    assert "confirmado y escrito" in result
    journal = (repo / "Y26" / "journal" / "mcblack.Y26.M8.journal").read_text(encoding="utf-8")
    assert "liabilities:tc:mcblack:saldo    -300.00 Q" in journal
    assert processor.confirm("CONFIRMAR", candidate["id"]).startswith("SMS ")


def test_confirm_commits_and_pushes_to_configured_remote(tmp_path, monkeypatch):
    repo = _copy_repo(tmp_path)
    subprocess.run(["git", "-C", str(repo), "init", "-q"], check=True)
    subprocess.run(["git", "-C", str(repo), "config", "user.email", "test@example.com"], check=True)
    subprocess.run(["git", "-C", str(repo), "config", "user.name", "Test"], check=True)
    subprocess.run(["git", "-C", str(repo), "checkout", "-q", "-b", "fix/whatsapp-journal-publish"], check=True)
    subprocess.run(["git", "-C", str(repo), "add", "."], check=True)
    subprocess.run(["git", "-C", str(repo), "commit", "-qm", "chore: initialize test ledger"], check=True)
    remote = tmp_path / "remote.git"
    subprocess.run(["git", "init", "--bare", "-q", str(remote)], check=True)
    subprocess.run(["git", "-C", str(repo), "remote", "add", "origin", str(remote)], check=True)
    subprocess.run(["git", "-C", str(repo), "push", "-qu", "origin", "HEAD"], check=True)

    processor = MmpSmsWebhookProcessor(
        {
            "mmp_repo": str(repo),
            "pending_path": str(tmp_path / "pending.json"),
            "git_publish": True,
        }
    )
    candidate = processor.prepare(_payload())
    monkeypatch.setattr(processor, "_validate_journal", lambda: None)

    result = processor.confirm("CONFIRMAR", candidate["id"])

    assert "commit" in result.lower()
    assert "push" in result.lower()
    log = subprocess.check_output(["git", "-C", str(repo), "log", "-1", "--format=%s"], text=True).strip()
    assert candidate["id"] in log
    branch = subprocess.check_output(["git", "-C", str(repo), "branch", "--show-current"], text=True).strip()
    local_sha = subprocess.check_output(["git", "-C", str(repo), "rev-parse", "HEAD"], text=True).strip()
    remote_sha = subprocess.check_output(["git", "-C", str(repo), "ls-remote", "origin", f"refs/heads/{branch}"], text=True).split()[0]
    assert remote_sha == local_sha


def test_confirm_refuses_protected_main_branch(tmp_path):
    repo = _copy_repo(tmp_path)
    subprocess.run(["git", "-C", str(repo), "init", "-q"], check=True)
    subprocess.run(["git", "-C", str(repo), "config", "user.email", "test@example.com"], check=True)
    subprocess.run(["git", "-C", str(repo), "config", "user.name", "Test"], check=True)
    subprocess.run(["git", "-C", str(repo), "checkout", "-q", "-b", "main"], check=True)
    subprocess.run(["git", "-C", str(repo), "add", "."], check=True)
    subprocess.run(["git", "-C", str(repo), "commit", "-qm", "chore: initialize test ledger"], check=True)
    processor = MmpSmsWebhookProcessor(
        {
            "mmp_repo": str(repo),
            "pending_path": str(tmp_path / "pending.json"),
            "git_publish": True,
        }
    )
    candidate = processor.prepare(_payload())

    result = processor.confirm("CONFIRMAR", candidate["id"])

    assert "branch protegida main" in result
    assert "300.00 Q" not in (repo / "Y26" / "journal" / "mcblack.Y26.M8.journal").read_text(encoding="utf-8")


def test_aiohttp_route_returns_200_and_queues_preview(tmp_path):
    repo = _copy_repo(tmp_path)
    config = PlatformConfig.from_dict(
        {
            "enabled": True,
            "extra": {
                "host": "127.0.0.1",
                "port": 0,
                "mmp_sms": {
                    "enabled": True,
                    "mmp_repo": str(repo),
                    "pending_path": str(tmp_path / "pending.json"),
                    "allowed_ips": ["127.0.0.1"],
                },
            },
        }
    )
    adapter = WebhookAdapter(config)
    adapter.gateway_runner = SimpleNamespace(adapters={})
    sent = []

    async def fake_preview(candidate, runner):
        sent.append(candidate["id"])

    adapter._mmp_sms_processor.send_preview = fake_preview

    async def exercise():
        assert await adapter.connect() is True
        port = adapter._runner.addresses[0][1]
        async with aiohttp.ClientSession() as session:
            async with session.post(f"http://127.0.0.1:{port}/webhook", json=_payload()) as response:
                assert response.status == 200
                assert await response.json() == {"ok": True}
        await asyncio.sleep(0.2)
        await adapter.disconnect()

    asyncio.run(exercise())
    pending = adapter._mmp_sms_processor._read_pending()
    assert pending
    candidate = next(iter(pending.values()))
    assert candidate.get("status") == "queued_for_agent"
    assert candidate.get("body")
    journal = (repo / "Y26" / "journal" / "mcblack.Y26.M8.journal").read_text(encoding="utf-8")
    assert "300.00 Q" not in journal


def test_ingest_raw_does_not_parse_gtc_sporta(tmp_path):
    repo = _copy_repo(tmp_path)
    processor = MmpSmsWebhookProcessor(
        {"mmp_repo": str(repo), "pending_path": str(tmp_path / "pending.json")}
    )
    raw = processor.ingest_raw(
        {
            "type": "sms_transaction_notification",
            "user": "carlos",
            "sender": "50254001718",
            "body": "BANCO GTC: Consumo tarjeta credito con la cuenta 7664  No. autorizacion: 00088542  Monto: Q. 820.00 Localidad: SPORTA GUATEMALA         GUATEMALA    GT",
            "receivedAt": "2026-08-31 16:30:23",
            "source": "termux-sms",
        }
    )
    assert raw["status"] == "queued_for_agent"
    assert "SPORTA GUATEMALA" in raw["body"]
    assert "account" not in raw or raw.get("account") is None
    prompt = processor.llm_prompt([raw])
    assert "TÚ parseas" in prompt
    assert "SPORTA GUATEMALA" in prompt
    assert "cuenta 7664" in prompt


def test_otp_and_transfers_are_ignored(tmp_path):
    repo = _copy_repo(tmp_path)
    processor = MmpSmsWebhookProcessor(
        {"mmp_repo": str(repo), "pending_path": str(tmp_path / "pending.json")}
    )
    otp = processor.prepare(
        _payload("BamAvisa: Valida el inicio de sesion de tu Banca Virtual ingresando el codigo 443315.")
        | {"sender": "50242149801"}
    )
    assert otp["disposition"] == "ignore"
    assert processor.apply_policy(otp) == "ignore"

    transfer = processor.prepare(
        _payload("BiMovil: Debito por Q.10,395.22 Cuenta MONE1 en la Agencia DIGITAL  31-Ago 12:05 Autorizacion 188995.")
        | {"sender": "+2424"}
    )
    assert transfer["disposition"] == "ignore"
    assert processor.apply_policy(transfer) == "ignore"


def test_ficoaviso_known_merchant_is_auto(tmp_path):
    repo = _copy_repo(tmp_path)
    (repo / "Y26" / "journal" / "ficohsa.Y26.M8.journal").write_text(
        "2026-08-01 SUPER  ; cc:0124 cur:Q\n", encoding="utf-8"
    )
    processor = MmpSmsWebhookProcessor(
        {"mmp_repo": str(repo), "pending_path": str(tmp_path / "pending.json")}
    )
    candidate = processor.prepare(
        _payload(
            "FICOAVISO: Transaccion TC xx0124 por Q 155.65 en PRICESMART FRAIJANES, si no la reconoce llame al 23178444."
        )
        | {"sender": "50255137741"}
    )
    assert candidate["disposition"] == "auto"
    assert candidate["needs_review"] is False
    assert candidate["account"] == "ficohsa"
    assert candidate["category"] == "Super"
    assert processor.apply_policy(candidate) == "auto"
    journal = (repo / "Y26" / "journal" / "ficohsa.Y26.M8.journal").read_text(encoding="utf-8")
    assert "liabilities:tc:ficohsa:saldo    -155.65 Q" in journal


def test_unknown_merchant_purchase_asks_confirmation(tmp_path):
    repo = _copy_repo(tmp_path)
    processor = MmpSmsWebhookProcessor(
        {"mmp_repo": str(repo), "pending_path": str(tmp_path / "pending.json")}
    )
    candidate = processor.prepare(
        _payload("Compra en COMERCIO NUEVO por Q 125.00") | {"sender": "BAM"}
    )
    assert candidate["disposition"] == "review"
    assert processor.apply_policy(candidate) == "preview"


def test_format_budget_status_fijo_monthly_only():
    text = format_budget_status(
        {
            "name": "CASA",
            "budgetTypeId": "fijo",
            "status": "on_track",
            "target": {"amount": "10395.22", "currency": "GTQ"},
            "actual": {"amount": "10395.22", "currency": "GTQ"},
        },
        category_label="CASA",
    )
    assert "Mes: 100% ejecutado" in text
    assert "Semana:" not in text
    assert "CASA" in text


def test_format_budget_status_variable_week_and_over():
    text = format_budget_status(
        {
            "name": "Transporte",
            "budgetTypeId": "variable",
            "status": "over",
            "target": {"amount": "800.00", "currency": "GTQ"},
            "actual": {"amount": "880.00", "currency": "GTQ"},
            "pacing": {
                "cadence": "weekly",
                "status": "over",
                "currentPeriodIndex": 1,
                "spentThisPeriod": {"amount": "250.00", "currency": "GTQ"},
                "allowanceRemaining": {"amount": "0", "currency": "GTQ"},
                "periods": [
                    {
                        "index": 1,
                        "allowance": {"amount": "200.00", "currency": "GTQ"},
                    }
                ],
            },
        },
        category_label="Transporte",
    )
    assert "EXCEDIDO" in text
    assert "Mes: 110% ejecutado" in text
    assert "Semana: 125% ejecutado" in text


def test_auto_notice_includes_budget_when_api_returns_line(tmp_path, monkeypatch):
    repo = _copy_repo(tmp_path)
    processor = MmpSmsWebhookProcessor(
        {"mmp_repo": str(repo), "pending_path": str(tmp_path / "pending.json")}
    )
    candidate = processor.prepare(_payload())
    candidate["notice"] = f"✅ SMS {candidate['id']} confirmado y escrito en mcblack.Y26.M8.journal."
    monkeypatch.setattr(
        processor,
        "fetch_budget_month",
        lambda year_month: {
            "yearMonth": year_month,
            "lines": [
                {
                    "name": "Transporte",
                    "budgetTypeId": "variable",
                    "status": "on_track",
                    "target": {"amount": "800.00", "currency": "GTQ"},
                    "actual": {"amount": "300.00", "currency": "GTQ"},
                    "pacing": {
                        "status": "on_track",
                        "currentPeriodIndex": 0,
                        "spentThisPeriod": {"amount": "80.00", "currency": "GTQ"},
                        "periods": [{"index": 0, "allowance": {"amount": "200.00", "currency": "GTQ"}}],
                    },
                }
            ],
        },
    )
    notice = processor.auto_notice(candidate)
    assert "Registré automáticamente" in notice
    assert "Mes: 38% ejecutado" in notice
    assert "Semana: 40% ejecutado" in notice
