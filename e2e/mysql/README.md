# Local MySQL fixture

## Create local credentials once

Local initialization and the default local Datly transcription/validation use
Endly credentials from `~/.secret/mysql-e2e.json`.

Check for an existing file first: it may already be shared with Agently Core or
other E2E workflows. Reuse it when its credentials match the gateway's local
MySQL server. Do not overwrite a shared file blindly or change it simply to
reset this fixture's password.

If the file does not exist, create it with the native Endly command:

```sh
endly -c=mysql-e2e
```

When prompted, enter username `root` and password `dev` for the standard local
fixture. Endly writes `~/.secret/mysql-e2e.json`; its native credential loader
handles the stored format, including encrypted passwords. Do not handwrite
plaintext JSON credentials. An existing volume retains its original MySQL
password, so the credential must match that password. Creating a credential file
does not change the server's password.

If an existing shared `mysql-e2e.json` belongs to another server or uses a
different password, create a separate credential instead:

```sh
endly -c=mysql-slack-e2e
```

Enter the gateway server's username and password. This creates
`~/.secret/mysql-slack-e2e.json` without changing `mysql-e2e.json`. Pass
`mysqlCred=mysql-slack-e2e.json` to select it. An absolute credential-file path,
such as `mysqlCred=/absolute/path/another.json`, is also supported.

## Initialize the fixture

Run from `e2e/`:

```sh
endly -t=init
```

With the separate credential:

```sh
endly -t=init mysqlCred=mysql-slack-e2e.json
```

The workflow starts MySQL 8.0 with the project's own container and persistent
volume, binds `127.0.0.1:3309`, and applies the versioned DDL to
`agently_steward_slack`. Repeating initialization preserves existing rows.
This explicit workflow does not run during gateway startup. Use one gateway
process per database. The fixture is intended for local development only.

Configure the gateway through `storage.driver: mysql` and
`storage.dsn_env: AGENTLY_SLACK_MYSQL_DSN`. Set that environment variable to a
MySQL DSN targeting `127.0.0.1:3309/agently_steward_slack` with `parseTime=true`.
The gateway runtime uses its configured environment variable independently of
Endly's credential file. For the standard `root`/`dev` fixture, its DSN is
`root:dev@tcp(127.0.0.1:3309)/agently_steward_slack?parseTime=true`.
Use the original password when restarting an existing volume; the image does
not reset it. Keep real credentials outside version control.

After initialization, run from `e2e/datly/`:

```sh
endly -r=transcribe
endly -r=validate
```

Use the same `mysqlCred` override for both commands when initialization used a
separate credential:

```sh
endly -r=transcribe mysqlCred=mysql-slack-e2e.json
endly -r=validate mysqlCred=mysql-slack-e2e.json
```

These commands use the same Endly credentials and existing local server; they
do not start Docker or provision a database. See the
[Datly workflow instructions](../datly/README.md).
For standalone transcription/validation, an explicit `dsn` workflow parameter
takes precedence over a nonempty `AGENTLY_SLACK_MYSQL_DSN`; either overrides the
credential-based local connection and bypasses the personal credential file.
These overrides do not change local provisioning or the password in an existing
volume. See the Datly workflow instructions for connection selection and logging.

Run `endly -t=verify` for schema acceptance on a separate temporary container.
It checks versioning, repeat initialization, uniqueness, case sensitivity,
foreign keys, CHECK constraints and large Unicode payloads. It does not modify
the persistent local fixture or require the personal MySQL credential file or
Slack/Agently credentials. The full `endly -r=run` workflow in `e2e/datly/` also
uses its own isolated fixture and credentials.
