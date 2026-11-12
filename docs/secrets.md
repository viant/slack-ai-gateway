# Secret configuration

The gateway reads configured Scy resources rather than Slack tokens, OOB
references or MySQL DSNs from environment variables. Keep the runtime YAML
outside version control: resource names and database locations may themselves
be sensitive. `validate-config` checks the configuration offline; resource
contents are loaded only by commands that need them.

## Prepare local resources

Install the Scy CLI used by your environment. Check for existing resources
before creating anything; reuse a compatible credential instead of overwriting
another application's file.

```sh
install -d -m 700 "$HOME/.secret/slack-ai-gateway"
umask 077
```

The following commands prompt interactively. Enter the Slack token twice;
the `basic` target asks for a username and password. Do not put real values in
command arguments, shell history or temporary plaintext files. Run each
creation command only when its destination does not already exist.

```sh
scy secure --target raw --key blowfish://default \
  --dest "file://$HOME/.secret/slack-ai-gateway/slack-app-token.enc"
scy secure --target raw --key blowfish://default \
  --dest "file://$HOME/.secret/slack-ai-gateway/slack-bot-token.enc"
scy secure --target basic --key blowfish://default \
  --dest "file://$HOME/.secret/slack-ai-gateway/mysql.json"
scy secure --target basic --key blowfish://default \
  --dest "file://$HOME/.secret/slack-ai-gateway/agently-service.json"
```

Use the execution account for `agently-service.json`; its authenticated subject
must match `agently.principal_id`. An existing compatible OOB credential file can
be referenced directly. Create `mysql.json` only when using MySQL, with the actual
credentials of that database. The standard local E2E fixture uses `root`/`dev`;
an existing volume retains its original password.

The raw token files contain encrypted text. A `basic` JSON resource contains
`Username` and `EncryptedPassword`; the username remains readable in that private
file. Scy decrypts the password when loading the resource.

Set created files to `0600`, and the private runtime YAML to `0600`. Use the
actual filenames when reusing resources:

```sh
chmod 600 "$HOME/.secret/slack-ai-gateway/"*.enc \
  "$HOME/.secret/slack-ai-gateway/"*.json
chmod 600 "$HOME/.slack-ai-gateway/config/gateway.local.yaml"
```

Replace the example locations in the YAML with absolute paths. The resource
keys are the native, case-sensitive Scy names `URL` and `Key`:

```yaml
slack:
  app_token_secret:
    URL: file:///absolute/path/slack-app-token.enc
    Key: blowfish://default
  bot_token_secret:
    URL: file:///absolute/path/slack-bot-token.enc
    Key: blowfish://default
agently:
  auth:
    mode: oob_session
    secret:
      URL: file:///absolute/path/agently-service.json
      Key: blowfish://default
storage:
  driver: mysql
  dsn: '${Username}:${Password}@tcp(127.0.0.1:3309)/slack_ai_gateway?parseTime=true&loc=UTC'
  secret:
    URL: file:///absolute/path/mysql.json
    Key: blowfish://default
```

This fragment supplements the IDs, agent, access policy and other settings in
the [runtime example](../examples/gateway.example.yaml). With SQLite, keep its
`storage.driver` and `storage.path`; no MySQL resource is needed. A complete MySQL
DSN without `storage.secret` is also supported for local development. Datly owns
the native `${Username}`/`${Password}` substitution; these are not environment
variables or general YAML interpolation.

## Production resources

The same fields can reference AWS Secrets Manager:

```yaml
storage:
  driver: mysql
  dsn: '${Username}:${Password}@tcp(mysql.internal:3306)/gateway_database?parseTime=true&loc=UTC&tls=true'
  secret:
    URL: aws://secretmanager/us-east-1/secret/production/example-gateway/mysql
    Key: blowfish://default
```

Use corresponding resources for the Slack tokens and OOB account. Publish through
a Scy CLI with its AWS provider linked, or through the native Scy/AFS writer used
by deployment automation. For example, this command prompts for a raw token and
creates or replaces the selected secret; run it only for an intended provisioning
or rotation operation:

```sh
scy secure --target raw --key blowfish://default \
  --dest aws://secretmanager/us-east-1/secret/production/example-gateway/app-token.enc
```

Use `--target basic` for MySQL and OOB credentials. The publishing identity needs
the relevant create/update permissions; the running gateway only needs read
access. The `tls=true` example assumes MySQL presents a certificate trusted by the
gateway.

The pinned AFS provider publishes ASCII payloads as `SecretString`. For other
payloads, including usual raw token ciphertext, it places base64 text in the
`SecretBinary` payload, in addition to the AWS SDK's transport encoding, and
removes that layer when reading. Native
Scy/AFS publication preserves this representation. A standard AWS binary upload
of the raw local `.enc` bytes does not provide that envelope and cannot be read
by this provider. Deployment tooling must use native publication or preserve its
representation; no gateway-specific decoding fallback is provided.

Provide the private YAML through deployment automation and grant the deployed
service identity access only to the required secrets and any applicable AWS KMS
keys. A role assigned to the workload avoids distributing static cloud keys
through application configuration. The standard AWS credential chain remains
owned by the AWS SDK; no gateway-specific AWS credential fields are introduced.

`blowfish://default` selects a shared key compiled into Scy. Treat AWS Secrets
Manager authorization, IAM and access to the private configuration as the access
boundary. This selection is separate from AWS Secrets Manager's encryption at
rest and does not select a managed application KMS adapter.

## Migration and rotation

The former `app_token_env`, `bot_token_env`, `secrets_url_env` and `dsn_env`
configuration fields are removed. A configuration containing them is rejected.
Replace them with resource mappings and the mixed DSN shown above. Exporting
the former runtime variables does not supply missing resource configuration.

Verify new resources without revealing their values: run `validate-config`,
then `check-agently`, and `status` for an initialized database. Do not use
`scy reveal` merely to check that setup succeeded. `check-agently` verifies the
session identity and protected agent listing without Slack or a conversation.

After rotating Slack tokens or MySQL credentials, restart the gateway to rebuild
its clients and connection pool. Each OOB login reads its configured resource
again, while the existing session recovery rules remain unchanged. Restart
after changing the YAML or correcting a blocked authentication setup. Preserve
existing sources until the replacement has been verified; no automatic
configuration or credential hot reload is provided.
