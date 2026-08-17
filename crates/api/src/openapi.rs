//! OpenAPI description.
//!
//! Hand-written rather than derived. The derive macros would require annotating
//! every DTO and would still not express the things that matter here — the
//! idempotency contract, the 200-vs-201 distinction, and which errors are
//! retryable — so a maintained document is the more honest option.

use axum::response::IntoResponse;
use axum::Json;
use serde_json::json;

pub async fn spec() -> impl IntoResponse {
    Json(json!({
      "openapi": "3.0.3",
      "info": {
        "title": "ChainRail API",
        "version": env!("CARGO_PKG_VERSION"),
        "description":
          "Multi-chain deposit and withdrawal infrastructure.\n\n\
           All monetary values are decimal strings of an asset's smallest \
           indivisible unit (raw units). JSON numbers are never used for money.\n\n\
           Errors share one shape: `{\"error\":{\"code\",\"message\",\"request_id\",\
           \"retryable\",\"details\"}}`. Branch on `code`, never on `message`."
      },
      "servers": [{ "url": "http://localhost:8088", "description": "local docker compose" }],
      "components": {
        "securitySchemes": {
          "bearer": {
            "type": "http",
            "scheme": "bearer",
            "description":
              "Required outside local/test. A placeholder for a real \
               authentication service; see docs/threat-model.md."
          }
        },
        "schemas": {
          "Error": {
            "type": "object",
            "properties": {
              "error": {
                "type": "object",
                "required": ["code", "message", "request_id"],
                "properties": {
                  "code": { "type": "string", "example": "insufficient_balance" },
                  "message": { "type": "string" },
                  "request_id": { "type": "string" },
                  "retryable": { "type": "boolean" },
                  "details": { "type": "object", "additionalProperties": true }
                }
              }
            }
          },
          "RawAmount": {
            "type": "string",
            "pattern": "^-?[0-9]+$",
            "description": "Integer in the asset's smallest unit, as a string.",
            "example": "25000000"
          },
          "Balance": {
            "type": "object",
            "properties": {
              "chain": { "type": "string" },
              "asset": { "type": "string" },
              "decimals": { "type": "integer" },
              "available_raw": { "$ref": "#/components/schemas/RawAmount" },
              "available_formatted": { "type": "string" },
              "reserved_raw": { "$ref": "#/components/schemas/RawAmount" },
              "total_raw": { "$ref": "#/components/schemas/RawAmount" },
              "deficit_raw": {
                "$ref": "#/components/schemas/RawAmount",
                "description": "Present only when a post-credit reorg created a receivable."
              }
            }
          },
          "Deposit": {
            "type": "object",
            "properties": {
              "id": { "type": "string", "format": "uuid" },
              "user_id": { "type": "string", "format": "uuid" },
              "chain": { "type": "string" },
              "asset": { "type": "string" },
              "amount_raw": { "$ref": "#/components/schemas/RawAmount" },
              "status": {
                "type": "string",
                "enum": ["observed", "confirming", "confirmed", "credited", "reorged", "failed"]
              },
              "confirmations": { "type": "integer" },
              "required_confirmations": { "type": "integer", "nullable": true },
              "tx_hash": { "type": "string", "nullable": true },
              "credited_at": { "type": "string", "format": "date-time", "nullable": true }
            }
          },
          "Withdrawal": {
            "type": "object",
            "properties": {
              "id": { "type": "string", "format": "uuid" },
              "user_id": { "type": "string", "format": "uuid" },
              "chain": { "type": "string" },
              "asset": { "type": "string" },
              "amount_raw": { "$ref": "#/components/schemas/RawAmount" },
              "destination": { "type": "string" },
              "status": {
                "type": "string",
                "enum": ["requested", "validated", "approved", "signing",
                         "broadcast", "confirming", "completed", "failed", "cancelled"]
              },
              "tx_hash": { "type": "string", "nullable": true },
              "confirmations": { "type": "integer" },
              "failure_code": { "type": "string", "nullable": true }
            }
          }
        }
      },
      "paths": {
        "/health": {
          "get": {
            "summary": "Liveness",
            "description": "Always 200 if the process is up. Reports the signer backend \
                            and whether it is production-grade (it never is in v0.1).",
            "responses": { "200": { "description": "Service is alive" } }
          }
        },
        "/ready": {
          "get": {
            "summary": "Readiness",
            "description": "503 only when the database is unreachable. Kafka and RPC \
                            outages are reported but do not fail readiness, because \
                            balance and history reads remain correct without them.",
            "responses": {
              "200": { "description": "Ready to serve" },
              "503": { "description": "A required dependency is down" }
            }
          }
        },
        "/metrics": {
          "get": {
            "summary": "Prometheus metrics",
            "responses": { "200": { "description": "Text exposition format" } }
          }
        },
        "/internal/ledger-integrity": {
          "get": {
            "summary": "Verify ledger invariants",
            "description": "Full scan. Returns 500 when any invariant is violated, so \
                            monitoring alerts even without reading the body.",
            "responses": {
              "200": { "description": "Ledger is consistent" },
              "500": { "description": "Integrity violation -- investigate immediately" }
            }
          }
        },
        "/v1/users": {
          "post": {
            "summary": "Create a user",
            "description": "Idempotent on `external_id`: 201 when created, 200 when it \
                            already existed.",
            "security": [{ "bearer": [] }],
            "requestBody": {
              "required": true,
              "content": { "application/json": { "schema": {
                "type": "object",
                "required": ["external_id"],
                "properties": { "external_id": { "type": "string", "maxLength": 128 } }
              }}}
            },
            "responses": {
              "201": { "description": "Created" },
              "200": { "description": "Already existed" },
              "400": { "description": "Validation error",
                       "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Error" }}}}
            }
          }
        },
        "/v1/deposits": {
          "get": {
            "summary": "List deposits",
            "description": "Keyset pagination. Pass `after` and `after_id` from the \
                            previous page's `next_cursor`.",
            "security": [{ "bearer": [] }],
            "parameters": [
              { "name": "user_id", "in": "query", "schema": { "type": "string", "format": "uuid" }},
              { "name": "status", "in": "query", "schema": { "type": "string" }},
              { "name": "limit", "in": "query", "schema": { "type": "integer", "maximum": 200 }},
              { "name": "after", "in": "query", "schema": { "type": "string", "format": "date-time" }},
              { "name": "after_id", "in": "query", "schema": { "type": "string", "format": "uuid" }}
            ],
            "responses": { "200": { "description": "A page of deposits" }}
          }
        },
        "/v1/deposits/{id}": {
          "get": {
            "summary": "Get a deposit",
            "security": [{ "bearer": [] }],
            "parameters": [{ "name": "id", "in": "path", "required": true,
                             "schema": { "type": "string", "format": "uuid" }}],
            "responses": {
              "200": { "description": "The deposit",
                       "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Deposit" }}}},
              "404": { "description": "Not found" }
            }
          }
        },
        "/v1/transactions/{hash}": {
          "get": {
            "summary": "Get observed transfers for a transaction hash",
            "security": [{ "bearer": [] }],
            "parameters": [{ "name": "hash", "in": "path", "required": true,
                             "schema": { "type": "string" }}],
            "responses": {
              "200": { "description": "Transfers in that transaction" },
              "400": { "description": "Malformed hash" },
              "404": { "description": "No tracked transfers in that transaction" }
            }
          }
        },
        "/v1/balances/{user_id}": {
          "get": {
            "summary": "Get balances",
            "description": "Derived from the ledger. `available` is spendable; \
                            `reserved` is locked against in-flight withdrawals.",
            "security": [{ "bearer": [] }],
            "parameters": [{ "name": "user_id", "in": "path", "required": true,
                             "schema": { "type": "string", "format": "uuid" }}],
            "responses": {
              "200": { "description": "Balances per chain and asset" },
              "404": { "description": "Unknown user" }
            }
          }
        },
        "/v1/ledger/{user_id}": {
          "get": {
            "summary": "Get ledger entries",
            "description": "Immutable statement: every entry against the user's accounts, \
                            newest first, with the running balance after each.",
            "security": [{ "bearer": [] }],
            "parameters": [
              { "name": "user_id", "in": "path", "required": true,
                "schema": { "type": "string", "format": "uuid" }},
              { "name": "limit", "in": "query", "schema": { "type": "integer" }}
            ],
            "responses": { "200": { "description": "A page of ledger entries" }}
          }
        },
        "/v1/withdrawals": {
          "post": {
            "summary": "Request a withdrawal",
            "description":
              "Idempotent on `(user_id, idempotency_key)`. 201 when created, 200 when \
               the same key and body were already submitted. Reusing a key with a \
               *different* body returns 409 `idempotency_key_conflict` rather than \
               silently returning the earlier withdrawal.\n\n\
               A policy denial returns 422 with the specific rule in \
               `error.details.policy`.",
            "security": [{ "bearer": [] }],
            "requestBody": {
              "required": true,
              "content": { "application/json": { "schema": {
                "type": "object",
                "required": ["user_id", "chain", "asset", "amount", "destination", "idempotency_key"],
                "properties": {
                  "user_id": { "type": "string", "format": "uuid" },
                  "chain": { "type": "string", "example": "base-sepolia" },
                  "asset": { "type": "string", "example": "USDC" },
                  "amount": { "$ref": "#/components/schemas/RawAmount" },
                  "destination": { "type": "string", "example": "0x5aAeb6053F3E94C9b9A09f33669435E7Ef1BeAed" },
                  "idempotency_key": { "type": "string", "minLength": 8, "maxLength": 128 }
                }
              }}}
            },
            "responses": {
              "201": { "description": "Created",
                       "content": { "application/json": { "schema": { "$ref": "#/components/schemas/Withdrawal" }}}},
              "200": { "description": "Idempotent replay of an identical request" },
              "400": { "description": "Validation error, including a bad EIP-55 checksum" },
              "409": { "description": "Insufficient balance, or idempotency key reused with a different body" },
              "422": { "description": "Denied by policy" }
            }
          },
          "get": {
            "summary": "List withdrawals",
            "security": [{ "bearer": [] }],
            "responses": { "200": { "description": "A page of withdrawals" }}
          }
        },
        "/v1/withdrawals/{id}": {
          "get": {
            "summary": "Get a withdrawal",
            "security": [{ "bearer": [] }],
            "parameters": [{ "name": "id", "in": "path", "required": true,
                             "schema": { "type": "string", "format": "uuid" }}],
            "responses": {
              "200": { "description": "The withdrawal" },
              "404": { "description": "Not found" }
            }
          }
        },
        "/v1/withdrawals/{id}/cancel": {
          "post": {
            "summary": "Cancel a withdrawal and release its reservation",
            "description": "Only legal before broadcast. A broadcast transaction cannot \
                            be recalled, so this returns 409 once funds may have left.",
            "security": [{ "bearer": [] }],
            "parameters": [{ "name": "id", "in": "path", "required": true,
                             "schema": { "type": "string", "format": "uuid" }}],
            "responses": {
              "200": { "description": "Cancelled and funds released" },
              "409": { "description": "Already broadcast or terminal" }
            }
          }
        },
        "/v1/withdrawals/{id}/approve": {
          "post": {
            "summary": "Approve a withdrawal held for manual review",
            "security": [{ "bearer": [] }],
            "parameters": [{ "name": "id", "in": "path", "required": true,
                             "schema": { "type": "string", "format": "uuid" }}],
            "responses": {
              "200": { "description": "Approved; the pipeline will broadcast it" },
              "409": { "description": "Not awaiting approval" }
            }
          }
        }
      }
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn spec_is_valid_json_and_documents_every_public_route() {
        let response = spec().await;
        let body = axum::body::to_bytes(response.into_response().into_body(), usize::MAX)
            .await
            .unwrap();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(v["openapi"], "3.0.3");
        let paths = v["paths"].as_object().expect("paths object");
        for route in [
            "/health",
            "/ready",
            "/metrics",
            "/v1/users",
            "/v1/deposits",
            "/v1/deposits/{id}",
            "/v1/transactions/{hash}",
            "/v1/balances/{user_id}",
            "/v1/ledger/{user_id}",
            "/v1/withdrawals",
            "/v1/withdrawals/{id}",
        ] {
            assert!(paths.contains_key(route), "undocumented route: {route}");
        }
    }

    #[tokio::test]
    async fn spec_documents_the_idempotency_contract() {
        let response = spec().await;
        let body = axum::body::to_bytes(response.into_response().into_body(), usize::MAX)
            .await
            .unwrap();
        let text = String::from_utf8(body.to_vec()).unwrap();
        assert!(text.contains("idempotency_key_conflict"));
        assert!(text.contains("raw units"));
    }
}
