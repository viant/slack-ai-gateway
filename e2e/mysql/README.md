# Local MySQL fixture

Run from `e2e/` after exporting a local password:

```sh
export AGENTLY_SLACK_MYSQL_PASSWORD='<local test password>'
endly -t=init
```

The workflow starts MySQL 8.0 with the project's own container and persistent
volume, binds `127.0.0.1:3309`, and applies the versioned DDL to
`agently_steward_slack`. Repeating initialization preserves existing rows.
This explicit workflow does not run during gateway startup. Use one gateway
process per database. The fixture is intended for local development only.

Configure the gateway through `storage.driver: mysql` and
`storage.dsn_env: AGENTLY_SLACK_MYSQL_DSN`. Set that environment variable to a
MySQL DSN targeting `127.0.0.1:3309/agently_steward_slack` with `parseTime=true`.
Keep passwords and complete DSNs outside version control. Reuse the original
password when restarting an existing volume; the image does not reset it.

Run `endly -t=verify` for schema acceptance on a separate temporary container.
It checks versioning, repeat initialization, uniqueness, case sensitivity,
foreign keys, CHECK constraints and large Unicode payloads. It does not modify
the persistent local fixture or require Slack/Agently credentials.
