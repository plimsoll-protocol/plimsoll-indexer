# Deployment

The indexer is a single HTTP service that also runs its ingest, reconcile,
stellar.toml and (optionally) supply-posting workers. It listens on `PORT`
(default `8080`) and serves `GET /healthz`. Use a persistent Postgres database:
the service applies schema migrations at startup, and the database holds the
event history and exact supply-breakdown bytes.

## Required configuration

Set these three variables for every deployment:

| Variable | Value |
| --- | --- |
| `DATABASE_URL` | PostgreSQL connection URL. Prefer the provider's private/internal URL when the database and service share a region. |
| `COVERAGE_LEDGER_ID` | Coverage contract ID for the selected Stellar network. |
| `REPORTER_REGISTRY_ID` | Reporter registry contract ID for the selected Stellar network. |

`NETWORK` defaults to `testnet`, which supplies the Stellar RPC URL, Horizon URL
and network passphrase. For `mainnet`, also set `STELLAR_RPC_URL` to a Soroban
RPC provider; the app has no public mainnet RPC default. If overriding a preset,
keep `STELLAR_RPC_URL`, `HORIZON_URL` and `STELLAR_NETWORK_PASSPHRASE` matched
to the same network. Set `START_LEDGER` to the deployment's starting ledger if
needed; RPC event retention is about seven days.

`SUPPLY_POSTER_SECRET` is optional. Leave it unset for a read-only deployment
(API, ingest, reconcile and TOML sync). To post supply, set it to the secret
seed of a funded account registered with the reporter registry's `SupplyPoster`
role. Never put a seed in this guide, source control, an image, or a checked-in
environment file.

Instead of environment variables, the poster secret and the database password
can come from a secrets backend. Set `SECRETS_PROVIDER=ssm` (AWS Systems Manager
Parameter Store) or `gcp` (GCP Secret Manager) and name the parameter or secret
in `SUPPLY_POSTER_SECRET_NAME` and `DATABASE_PASSWORD_SECRET_NAME`. Values are
fetched once at startup and never logged. Leave a name unset to keep that
credential in its environment variable. See the
[operations runbook](runbook.md#secrets-backend) for the provider setup details.

Optional webhook administration requires both `WEBHOOK_ADMIN_TOKEN` and
`WEBHOOK_ENCRYPTION_KEY`. The latter must be a base64-encoded 32-byte key; for
example, generate one with `openssl rand -base64 32`. Keep it stable because
changing it makes existing webhook secrets unreadable.

See the [operations runbook](runbook.md) and [.env.example](../.env.example)
for the remaining optional settings and defaults.

## Docker image

Releases publish a multi-architecture image to GitHub Container Registry:

```sh
docker pull ghcr.io/plimsoll-protocol/plimsoll-indexer:latest
```

The image is also built from the repository's [Dockerfile](../Dockerfile).
Render's included blueprint uses its Go build environment; Railway can build
from the Dockerfile or run the published GHCR image. Configure the environment
variables above in the hosting service rather than baking secrets into the
image. The container honors the provider-assigned `PORT`; its default is
`8080`, and `/healthz` is the health-check endpoint.

## Render

Use the one-click deployment button to create the web service and its Postgres
database from [render.yaml](../render.yaml):

[![Deploy to Render](https://render.com/images/deploy-to-render-button.svg)](https://render.com/deploy?repo=https://github.com/plimsoll-protocol/plimsoll-indexer)

The blueprint sets testnet contract IDs, start ledger and RPC/Horizon defaults.
Review these values before deploying to another network. It links the service
to the database using Render's internal connection string, and marks
`SUPPLY_POSTER_SECRET` as a secret that must be entered in the Render dashboard
after creation.

Render-specific notes:

- Keep the service and database in the same region so `DATABASE_URL` can use
  the private connection.
- Free web services can sleep when idle. On Render, the app defaults
  `KEEP_AWAKE_URL` to its own `$RENDER_EXTERNAL_URL/healthz` and pings it every
  ten minutes. This is intended to keep the poster running, but do not treat a
  free instance or free database as a production availability or durability
  guarantee.
- Render's free Postgres offering is temporary and has storage/availability
  limits. Check the current [Render Postgres pricing and limits](https://render.com/pricing)
  before using it; choose a paid persistent database for data you need to keep.
- To disable the self-ping, set `KEEP_AWAKE_URL=none`. Do not set it to an
  unrelated URL.

## Railway

Create a Railway project with an indexer service from this repository (or the
published GHCR image), then add a PostgreSQL service. In the indexer service's
Variables settings, set:

```text
DATABASE_URL=${{Postgres.DATABASE_URL}}
```

Replace `Postgres` with the exact name of the database service in your project.
Add `COVERAGE_LEDGER_ID`, `REPORTER_REGISTRY_ID`, and any network overrides as
described above. Railway resolves the database reference into the connection
URL; keep the two services in the same project and use the private service
connection.

Railway-specific notes:

- The app listens on the injected `PORT`; do not hard-code a port when
  configuring the generated public domain. Enable a public domain for the
  indexer service if you want to expose the HTTP API.
- Set a health check path of `/healthz` if configuring Railway health checks.
- `RENDER_EXTERNAL_URL` is Render-only, so Railway does not enable the
  self-ping automatically. Leave `KEEP_AWAKE_URL` unset unless you have a
  specific URL to ping.
- Railway plan names, trial credits and usage limits change. Confirm the
  current [Railway plans](https://railway.com/pricing) and database-volume
  persistence before relying on a no-cost deployment.

## Setting demo and runtime secrets

Add secrets in the hosting provider's service settings, not in `render.yaml`,
`railway.toml`, the Dockerfile, or committed source:

- **Render:** open the indexer web service's **Environment** settings and add
  each variable as a secret value, then save/redeploy. The Render deploy
  blueprint intentionally leaves `SUPPLY_POSTER_SECRET` for this step.
- **Railway:** open the indexer service's **Variables** settings and add each
  variable/value there, then deploy.

If a demo or workflow expects names such as `DEMO_TESTNET_SECRET` or
`DEMO_MAINNET_SECRET`, create each exact `DEMO_*_SECRET` name in the service
that runs that demo. They are not automatically aliases for the indexer's
poster key: this binary does not read a `DEMO_*_SECRET` variable. The indexer
reads `SUPPLY_POSTER_SECRET` specifically. If those demo values are needed by
a GitHub Actions workflow instead, add them under the repository's
**Settings → Secrets and variables → Actions**; GitHub Actions secrets are
not automatically passed to Render or Railway.

Only set `SUPPLY_POSTER_SECRET` when posting is intended. Use a testnet
SupplyPoster key for the blueprint's testnet configuration, ensure the account
has XLM for fees, and never reuse a mainnet key in a demo deployment.
