# MMPlus SMS

Adapter LAN para recibir SMS de Termux, entregarlos a Hermes y procesarlos con
la sesión del agente.

## Histórico durable

Cada recepción válida se añade a un archivo JSONL append-only antes de responder
`200` al teléfono:

```yaml
platforms:
  mmp_sms:
    extra:
      history:
        enabled: true
        path: ~/.hermes/mmp_sms/sms-history.jsonl
        disabled_users: []
```

Valores por defecto:

- `enabled: true`.
- `path: ~/.hermes/mmp_sms/sms-history.jsonl`.
- `disabled_users: []`.

Cada línea contiene el SMS completo (`sender`, `body`, `receivedAt`, `source`),
su fingerprint/ID y la hora de ingesta. Reintentos duplicados se conservan como
recepciones separadas con `duplicate: true`; no se pierde evidencia de entrega.
La escritura usa append, `fsync` y permisos `0600`. Si el archivo no puede
persistirse, el endpoint devuelve `503` y no confirma la recepción.

Para excluir a un usuario del archivo dedicado:

```yaml
history:
  enabled: true
  disabled_users:
    - carlos
```

El opt-out solo deshabilita este archivo histórico. El procesamiento normal de
Hermes todavía puede conservar el mensaje en `~/.hermes/state.db` como parte de
la sesión; deshabilitar también esa retención requiere la política general de
sesiones de Hermes.

## Flujo contable

El histórico y `mmp_sms_pending.json` son trazabilidad, no journal. Un `200`
solo confirma que la recepción fue persistida y puesta en cola. El journal solo
cambia después de que el agente procese el SMS y ejecute una escritura válida.
