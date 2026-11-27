package api

import "net/http"

// route is one endpoint: where it is served and how it is documented.
//
// Registration and documentation live in the same table so the two cannot
// drift. Handler serves exactly these routes and the OpenAPI document is
// generated from exactly these routes (see GeneratedOpenAPI), so adding an
// endpoint without documenting it, or documenting one that is not served, is
// not possible.
type route struct {
	Method  string
	Pattern string
	// Doc is the operation's OpenAPI body in YAML: summary, operationId,
	// parameters, responses. It is indented under the path and method when the
	// document is generated.
	Doc    string
	Handle func(*Server, http.ResponseWriter, *http.Request)
}

// routes returns the API's endpoints in the order they are documented.
func routes() []route {
	return []route{
		{
			Method:  "GET",
			Pattern: "/healthz",
			Handle:  (*Server).health,
			Doc: `
summary: Health check and indexer sync status
operationId: healthCheck
responses:
  '200':
    description: Indexer is healthy and synced
    content:
      application/json:
        schema:
          type: object
          properties:
            status:
              type: string
              example: "ok"
            ingested_to_ledger:
              type: integer
              example: 42
          required:
            - status
            - ingested_to_ledger
  '503':
    description: Storage/database is unavailable
    content:
      application/json:
        schema:
          type: object
          properties:
            status:
              type: string
              example: "db_unavailable"
          required:
            - status`,
		},
		{
			Method:  "GET",
			Pattern: "/openapi.yaml",
			Handle:  (*Server).openAPI,
			Doc: `
summary: Get OpenAPI 3.1 specification
operationId: getOpenAPISpec
responses:
  '200':
    description: OpenAPI specification file
    content:
      application/yaml:
        schema:
          type: string`,
		},
		{
			Method:  "GET",
			Pattern: "/v1/network",
			Handle:  (*Server).network,
			Doc: `
summary: Discover network parameters and contract IDs
operationId: getNetworkInfo
responses:
  '200':
    description: Network discovery parameters
    content:
      application/json:
        schema:
          $ref: '#/components/schemas/NetworkInfo'`,
		},
		{
			Method:  "GET",
			Pattern: "/v1/assets",
			Handle:  (*Server).listAssets,
			Doc: `
summary: List all tracked assets with coverage and reports
operationId: listAssets
parameters:
  - $ref: '#/components/parameters/MinTier'
responses:
  '200':
    description: List of tracked assets
    content:
      application/json:
        schema:
          type: object
          properties:
            assets:
              type: array
              items:
                $ref: '#/components/schemas/Asset'
          required:
            - assets
  '400':
    $ref: '#/components/responses/InvalidRequest'`,
		},
		{
			Method:  "GET",
			Pattern: "/v1/assets/{sac}",
			Handle:  (*Server).getAsset,
			Doc: `
summary: Get details, latest supply, reports, and coverage for a specific asset
operationId: getAsset
parameters:
  - $ref: '#/components/parameters/SAC'
  - $ref: '#/components/parameters/MinTier'
responses:
  '200':
    description: Asset details and current status
    content:
      application/json:
        schema:
          $ref: '#/components/schemas/Asset'
  '400':
    $ref: '#/components/responses/InvalidRequest'
  '404':
    $ref: '#/components/responses/AssetNotFound'`,
		},
		{
			Method:  "GET",
			Pattern: "/v1/assets/{sac}/supply",
			Handle:  (*Server).supplyHistory,
			Doc: `
summary: Get historical supply snapshots for an asset
operationId: getSupplyHistory
parameters:
  - $ref: '#/components/parameters/SAC'
  - $ref: '#/components/parameters/Limit'
responses:
  '200':
    description: Supply history snapshots
    content:
      application/json:
        schema:
          type: object
          properties:
            supply:
              type: array
              items:
                $ref: '#/components/schemas/Supply'
          required:
            - supply
  '400':
    description: Invalid limit parameter, or sac is not a contract strkey
    content:
      application/json:
        schema:
          $ref: '#/components/schemas/ErrorResponse'
  '404':
    $ref: '#/components/responses/AssetNotFound'`,
		},
		{
			Method:  "GET",
			Pattern: "/v1/assets/{sac}/reports",
			Handle:  (*Server).reportHistory,
			Doc: `
summary: Get historical reserve reports for an asset
operationId: getReportHistory
parameters:
  - $ref: '#/components/parameters/SAC'
  - $ref: '#/components/parameters/Limit'
responses:
  '200':
    description: Reserve report history
    content:
      application/json:
        schema:
          type: object
          properties:
            reports:
              type: array
              items:
                $ref: '#/components/schemas/Report'
          required:
            - reports
  '400':
    description: Invalid limit parameter, or sac is not a contract strkey
    content:
      application/json:
        schema:
          $ref: '#/components/schemas/ErrorResponse'
  '404':
    $ref: '#/components/responses/AssetNotFound'`,
		},
		{
			Method:  "GET",
			Pattern: "/v1/reporters",
			Handle:  (*Server).reporters,
			Doc: `
summary: List registered reporters and their roles
operationId: listReporters
responses:
  '200':
    description: List of registered reporters
    content:
      application/json:
        schema:
          type: object
          properties:
            reporters:
              type: array
              items:
                $ref: '#/components/schemas/Reporter'
          required:
            - reporters`,
		},
		{
			Method:  "GET",
			Pattern: "/v1/breakdowns/{hash}",
			Handle:  (*Server).breakdown,
			Doc: `
summary: Get raw supply breakdown by hash
operationId: getBreakdown
parameters:
  - $ref: '#/components/parameters/BreakdownHash'
responses:
  '200':
    description: Raw JSON breakdown served verbatim
    content:
      application/json:
        schema:
          type: object
  '400':
    description: Hash is not 64 hex characters
    content:
      application/json:
        schema:
          $ref: '#/components/schemas/ErrorResponse'
  '404':
    description: Breakdown hash not found
    content:
      application/json:
        schema:
          $ref: '#/components/schemas/ErrorResponse'`,
		},
		{
			Method:  "POST",
			Pattern: "/v1/webhooks",
			Handle:  (*Server).createWebhook,
			Doc: `
summary: Register an issuer TOML change webhook
operationId: createWebhook
security:
  - WebhookAdminAuth: []
requestBody:
  required: true
  content:
    application/json:
      schema:
        type: object
        properties:
          url:
            type: string
            format: uri
            example: "https://example.com/plimsoll-hook"
        required: [url]
responses:
  '201':
    description: Webhook created; secret is shown only in this response
    content:
      application/json:
        schema:
          $ref: '#/components/schemas/CreatedWebhook'
  '400':
    description: Invalid URL or request body
    content:
      application/json:
        schema:
          $ref: '#/components/schemas/ErrorResponse'
  '401':
    $ref: '#/components/responses/Unauthorized'
  '503':
    $ref: '#/components/responses/WebhooksNotConfigured'`,
		},
		{
			Method:  "GET",
			Pattern: "/v1/webhooks",
			Handle:  (*Server).listWebhooks,
			Doc: `
summary: List registered webhooks without their secrets
operationId: listWebhooks
security:
  - WebhookAdminAuth: []
responses:
  '200':
    description: Registered webhook destinations
    content:
      application/json:
        schema:
          type: object
          properties:
            webhooks:
              type: array
              items:
                $ref: '#/components/schemas/Webhook'
          required: [webhooks]
  '401':
    $ref: '#/components/responses/Unauthorized'
  '503':
    $ref: '#/components/responses/WebhooksNotConfigured'`,
		},
		{
			Method:  "DELETE",
			Pattern: "/v1/webhooks/{id}",
			Handle:  (*Server).deleteWebhook,
			Doc: `
summary: Delete a registered webhook
operationId: deleteWebhook
security:
  - WebhookAdminAuth: []
parameters:
  - $ref: '#/components/parameters/WebhookID'
responses:
  '204':
    description: Webhook deleted
  '400':
    description: Invalid webhook ID
  '401':
    $ref: '#/components/responses/Unauthorized'
  '404':
    description: Webhook not found`,
		},
	}
}
