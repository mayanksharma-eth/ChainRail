//! HTTP API behaviour: status codes, error shape, pagination, auth, limits.
//!
//! Exercises the real router with real middleware against a real database.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use chainrail_common::Amount;
use chainrail_integration::*;
use http_body_util::BodyExt;
use serde_json::{json, Value};
use tower::ServiceExt;
use uuid::Uuid;

async fn app(db: &chainrail_database::Db, token: Option<&str>) -> axum::Router {
    let mut cfg = test_app_config();
    cfg.http.api_token = token.map(String::from);
    // Register the configured assets, as the real boot path does.
    chainrail_api::state::sync_assets(db, &cfg).await.unwrap();

    let cfg = Arc::new(cfg);
    let rpc = chainrail_rpc::RpcRegistry::build(&cfg.chains).unwrap();
    let adapters = Arc::new(chainrail_chains_evm::build_adapters(&cfg.chains, &rpc).unwrap());
    let signer = chainrail_signer::from_config(&cfg.signer).unwrap();
    let bus = chainrail_events::build_bus(&cfg.kafka).unwrap();
    let risk = Arc::new(chainrail_risk::RiskEngine::new(cfg.risk.clone()));
    let withdrawals = chainrail_withdrawals::WithdrawalService::new(
        db.clone(),
        Arc::clone(&cfg),
        Arc::clone(&risk),
    );

    let state = Arc::new(chainrail_api::AppState {
        db: db.clone(),
        cfg,
        bus,
        rpc,
        adapters,
        signer,
        risk,
        withdrawals,
        metrics: None,
        started_at: std::time::Instant::now(),
    });
    chainrail_api::router(state)
}

async fn call(app: &axum::Router, req: Request<Body>) -> (StatusCode, Value) {
    let response = app.clone().oneshot(req).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let body: Value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(Value::Null)
    };
    (status, body)
}

fn get(path: &str) -> Request<Body> {
    Request::builder().uri(path).body(Body::empty()).unwrap()
}

fn post(path: &str, body: Value) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(path)
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap()
}

#[tokio::test]
async fn health_reports_the_signer_backend_and_never_claims_production_custody() {
    let db = require_db!();
    let app = app(&db, None).await;
    let (status, body) = call(&app, get("/health")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "ok");
    assert_eq!(body["signer_backend"], "local_development");
    assert_eq!(
        body["signer_production_grade"], false,
        "the API must never claim production-grade custody"
    );
}

#[tokio::test]
async fn readiness_is_green_when_the_database_is_reachable() {
    let db = require_db!();
    let app = app(&db, None).await;
    let (status, body) = call(&app, get("/ready")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["ready"], true);
    assert_eq!(body["database"]["ok"], true);
    assert_eq!(body["database"]["required"], true);
    // Kafka is reported but not required: its outage must not fail readiness.
    assert_eq!(body["event_bus"]["required"], false);
}

#[tokio::test]
async fn every_response_carries_a_request_id_header() {
    let db = require_db!();
    let app = app(&db, None).await;
    let response = app.clone().oneshot(get("/health")).await.unwrap();
    let id = response
        .headers()
        .get("x-request-id")
        .expect("x-request-id must be echoed");
    assert!(!id.to_str().unwrap().is_empty());

    // A client-supplied id is honoured, so client and server logs join up.
    let req = Request::builder()
        .uri("/health")
        .header("x-request-id", "client-req-42")
        .body(Body::empty())
        .unwrap();
    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.headers()["x-request-id"], "client-req-42");
}

#[tokio::test]
async fn user_creation_is_idempotent_with_distinct_status_codes() {
    let db = require_db!();
    let app = app(&db, None).await;

    let (status, body) = call(&app, post("/v1/users", json!({"external_id": "alice"}))).await;
    assert_eq!(status, StatusCode::CREATED);
    let user_id = body["id"].as_str().unwrap().to_string();

    // Same external_id: 200, not 201, and the same user.
    let (status, body) = call(&app, post("/v1/users", json!({"external_id": "alice"}))).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["id"].as_str().unwrap(), user_id);
}

#[tokio::test]
async fn validation_errors_use_the_standard_error_shape() {
    let db = require_db!();
    let app = app(&db, None).await;

    let (status, body) = call(&app, post("/v1/users", json!({"external_id": ""}))).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["code"], "validation_error");
    assert!(body["error"]["message"].is_string());
    assert!(body["error"]["request_id"].is_string());
}

#[tokio::test]
async fn unknown_ids_are_404_not_500() {
    let db = require_db!();
    let app = app(&db, None).await;
    for path in [
        &format!("/v1/users/{}", Uuid::new_v4()),
        &format!("/v1/deposits/{}", Uuid::new_v4()),
        &format!("/v1/withdrawals/{}", Uuid::new_v4()),
        &format!("/v1/balances/{}", Uuid::new_v4()),
    ] {
        let (status, body) = call(&app, get(path)).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "path {path}");
        assert_eq!(body["error"]["code"], "not_found");
    }
}

#[tokio::test]
async fn malformed_uuids_and_hashes_are_rejected_cleanly() {
    let db = require_db!();
    let app = app(&db, None).await;

    let (status, _) = call(&app, get("/v1/deposits/not-a-uuid")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let (status, body) = call(&app, get("/v1/transactions/0xdeadbeef")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["code"], "validation_error");
}

#[tokio::test]
async fn balances_are_derived_from_the_ledger_and_split_available_from_reserved() {
    let db = require_db!();
    let f = fixture(&db).await;
    let app = app(&db, None).await;

    fund(&db, f.user_id, f.asset_id, 100_000_000).await;
    let mut tx = db.begin().await.unwrap();
    chainrail_ledger::reserve_withdrawal(
        &mut tx,
        Uuid::new_v4(),
        f.user_id,
        f.asset_id,
        Amount::new(25_000_000),
        None,
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();

    let (status, body) = call(&app, get(&format!("/v1/balances/{}", f.user_id))).await;
    assert_eq!(status, StatusCode::OK);
    let usdc = body["balances"]
        .as_array()
        .unwrap()
        .iter()
        .find(|b| b["asset"] == "USDC")
        .expect("USDC balance");

    // Money is always a string, never a JSON number.
    assert_eq!(usdc["available_raw"], "75000000");
    assert_eq!(usdc["reserved_raw"], "25000000");
    assert_eq!(usdc["total_raw"], "100000000");
    assert_eq!(usdc["available_formatted"], "75");
    assert!(usdc["available_raw"].is_string());
}

#[tokio::test]
async fn the_ledger_endpoint_returns_an_immutable_statement() {
    let db = require_db!();
    let f = fixture(&db).await;
    let app = app(&db, None).await;
    fund(&db, f.user_id, f.asset_id, 50_000_000).await;

    let (status, body) = call(&app, get(&format!("/v1/ledger/{}", f.user_id))).await;
    assert_eq!(status, StatusCode::OK);
    let items = body["items"].as_array().unwrap();
    assert_eq!(items.len(), 1, "one entry against the user's account");
    assert_eq!(items[0]["direction"], "credit");
    assert_eq!(items[0]["amount_raw"], "50000000");
    assert_eq!(items[0]["account_type"], "user_available");
    assert_eq!(items[0]["balance_after_raw"], "50000000");
    assert_eq!(items[0]["kind"], "deposit_credit");
}

#[tokio::test]
async fn pagination_is_keyset_based_and_caps_the_page_size() {
    let db = require_db!();
    let f = fixture(&db).await;
    let app = app(&db, None).await;

    // 5 credits => 5 ledger entries on the user's account.
    for _ in 0..5 {
        fund(&db, f.user_id, f.asset_id, 1_000_000).await;
    }

    let (status, body) = call(&app, get(&format!("/v1/ledger/{}?limit=2", f.user_id))).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["items"].as_array().unwrap().len(), 2);
    let cursor = &body["next_cursor"];
    assert!(!cursor.is_null(), "a further page must advertise a cursor");

    let after = cursor["after"].as_str().unwrap();
    let after_id = cursor["after_id"].as_str().unwrap();
    let (status, page2) = call(
        &app,
        get(&format!(
            "/v1/ledger/{}?limit=2&after={}&after_id={}",
            f.user_id,
            urlencode(after),
            after_id
        )),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let page2_items = page2["items"].as_array().unwrap();
    assert_eq!(page2_items.len(), 2);
    // No overlap between pages.
    assert_ne!(page2_items[0]["id"], body["items"][0]["id"]);
    assert_ne!(page2_items[0]["id"], body["items"][1]["id"]);

    // An oversized limit is clamped rather than honoured.
    let (status, body) = call(&app, get(&format!("/v1/ledger/{}?limit=99999", f.user_id))).await;
    assert_eq!(status, StatusCode::OK);
    assert!(body["items"].as_array().unwrap().len() <= 200);
}

fn urlencode(s: &str) -> String {
    s.chars()
        .map(|c| match c {
            '+' => "%2B".to_string(),
            ':' => "%3A".to_string(),
            ' ' => "%20".to_string(),
            other => other.to_string(),
        })
        .collect()
}

#[tokio::test]
async fn withdrawal_creation_returns_201_then_200_for_the_same_key() {
    let db = require_db!();
    let f = fixture(&db).await;
    let app = app(&db, None).await;
    fund(&db, f.user_id, f.asset_id, 100_000_000).await;

    let body = json!({
        "user_id": f.user_id,
        "chain": TEST_CHAIN,
        "asset": "USDC",
        "amount": "25000000",
        "destination": "0x70997970C51812dc3A010C7d01b50e0d17dc79C8",
        "idempotency_key": "api-key-000001"
    });

    let (status, created) = call(&app, post("/v1/withdrawals", body.clone())).await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(created["amount_raw"], "25000000");
    assert_eq!(created["amount_formatted"], "25");
    // The destination comes back EIP-55 checksummed.
    assert_eq!(
        created["destination"], "0x70997970C51812dc3A010C7d01b50e0d17dc79C8",
        "destination must be returned checksummed"
    );

    let (status, replay) = call(&app, post("/v1/withdrawals", body)).await;
    assert_eq!(status, StatusCode::OK, "a replay must not be 201");
    assert_eq!(replay["id"], created["id"]);
}

#[tokio::test]
async fn reusing_a_key_with_a_different_body_is_a_conflict() {
    let db = require_db!();
    let f = fixture(&db).await;
    let app = app(&db, None).await;
    fund(&db, f.user_id, f.asset_id, 100_000_000).await;

    let base = json!({
        "user_id": f.user_id,
        "chain": TEST_CHAIN,
        "asset": "USDC",
        "amount": "10000000",
        "destination": "0x70997970C51812dc3A010C7d01b50e0d17dc79C8",
        "idempotency_key": "api-conflict-01"
    });
    let (status, _) = call(&app, post("/v1/withdrawals", base.clone())).await;
    assert_eq!(status, StatusCode::CREATED);

    let mut different = base;
    different["amount"] = json!("99000000");
    let (status, body) = call(&app, post("/v1/withdrawals", different)).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"]["code"], "idempotency_key_conflict");
}

#[tokio::test]
async fn a_bad_checksum_destination_is_rejected_with_a_specific_reason() {
    let db = require_db!();
    let f = fixture(&db).await;
    let app = app(&db, None).await;
    fund(&db, f.user_id, f.asset_id, 100_000_000).await;

    let (status, body) = call(
        &app,
        post(
            "/v1/withdrawals",
            json!({
                "user_id": f.user_id,
                "chain": TEST_CHAIN,
                "asset": "USDC",
                "amount": "1000000",
                // one character's case flipped
                "destination": "0x70997970c51812dc3A010C7d01b50e0d17dc79C8",
                "idempotency_key": "bad-checksum-01"
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["code"], "invalid_address");
    assert!(
        body["error"]["details"]["reason"]
            .as_str()
            .unwrap()
            .contains("checksum"),
        "the reason must name the checksum: {body}"
    );
}

#[tokio::test]
async fn insufficient_balance_is_a_422_with_the_policy_code() {
    let db = require_db!();
    let f = fixture(&db).await;
    let app = app(&db, None).await;
    fund(&db, f.user_id, f.asset_id, 1_000_000).await;

    let (status, body) = call(
        &app,
        post(
            "/v1/withdrawals",
            json!({
                "user_id": f.user_id,
                "chain": TEST_CHAIN,
                "asset": "USDC",
                "amount": "99000000",
                "destination": "0x70997970C51812dc3A010C7d01b50e0d17dc79C8",
                "idempotency_key": "too-poor-00001"
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(body["error"]["code"], "policy_denied");
    assert_eq!(body["error"]["details"]["policy"], "insufficient_balance");
}

#[tokio::test]
async fn float_amounts_are_rejected_outright() {
    let db = require_db!();
    let f = fixture(&db).await;
    let app = app(&db, None).await;
    fund(&db, f.user_id, f.asset_id, 100_000_000).await;

    // A JSON float can silently lose precision above 2^53; accepting one on a
    // money path would be a correctness bug, so it must not parse.
    let (status, _) = call(
        &app,
        post(
            "/v1/withdrawals",
            json!({
                "user_id": f.user_id,
                "chain": TEST_CHAIN,
                "asset": "USDC",
                "amount": 25.5,
                "destination": "0x70997970C51812dc3A010C7d01b50e0d17dc79C8",
                "idempotency_key": "float-amount-1"
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn authentication_is_enforced_on_v1_but_not_on_probes() {
    let db = require_db!();
    let app = app(&db, Some("s3cret-token")).await;

    // No credentials.
    let (status, _) = call(&app, get("/v1/deposits")).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    // Wrong credentials.
    let req = Request::builder()
        .uri("/v1/deposits")
        .header("authorization", "Bearer wrong")
        .body(Body::empty())
        .unwrap();
    let (status, _) = call(&app, req).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    // Correct credentials.
    let req = Request::builder()
        .uri("/v1/deposits")
        .header("authorization", "Bearer s3cret-token")
        .body(Body::empty())
        .unwrap();
    let (status, _) = call(&app, req).await;
    assert_eq!(status, StatusCode::OK);

    // Probes stay open so a load balancer can reach them.
    for probe in ["/health", "/ready"] {
        let (status, _) = call(&app, get(probe)).await;
        assert_eq!(status, StatusCode::OK, "{probe} must not require auth");
    }
}

#[tokio::test]
async fn oversized_request_bodies_are_refused() {
    let db = require_db!();
    let app = app(&db, None).await;

    // 128 KiB against a 64 KiB limit.
    let huge = json!({ "external_id": "x".repeat(128 * 1024) });
    let (status, _) = call(&app, post("/v1/users", huge)).await;
    assert!(
        status == StatusCode::PAYLOAD_TOO_LARGE || status == StatusCode::BAD_REQUEST,
        "expected a size rejection, got {status}"
    );
}

#[tokio::test]
async fn the_ledger_integrity_endpoint_reports_a_clean_ledger() {
    let db = require_db!();
    let f = fixture(&db).await;
    let app = app(&db, None).await;
    fund(&db, f.user_id, f.asset_id, 1_000).await;

    let (status, body) = call(&app, get("/internal/ledger-integrity")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["unbalanced_transactions"].as_array().unwrap().len(), 0);
    assert_eq!(body["balance_drift"].as_array().unwrap().len(), 0);
    assert_eq!(
        body["illegal_negative_balances"].as_array().unwrap().len(),
        0
    );
}

#[tokio::test]
async fn deposit_addresses_are_assigned_from_the_pool_and_are_idempotent() {
    let db = require_db!();
    let app = app(&db, None).await;

    let (_, user) = call(&app, post("/v1/users", json!({"external_id": "pool-user"}))).await;
    let user_id = user["id"].as_str().unwrap().to_string();

    // Empty pool: an operational failure, not the client's fault.
    let (status, body) = call(
        &app,
        post(
            "/v1/deposit-addresses",
            json!({"user_id": user_id, "chain": TEST_CHAIN}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["error"]["code"], "service_unavailable");

    // Custody exports an address into the pool.
    chainrail_database::repo::reference::add_pool_address(
        db.pool(),
        TEST_CHAIN,
        &address(0x42),
        Some("m/44'/60'/0'/0/9"),
    )
    .await
    .unwrap();

    let (status, first) = call(
        &app,
        post(
            "/v1/deposit-addresses",
            json!({"user_id": user_id, "chain": TEST_CHAIN}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let assigned = first["address"].as_str().unwrap().to_string();
    assert!(assigned.starts_with("0x"));

    // Repeating must return the same address, not burn a second one.
    let (status, again) = call(
        &app,
        post(
            "/v1/deposit-addresses",
            json!({"user_id": user_id, "chain": TEST_CHAIN}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(again["address"].as_str().unwrap(), assigned);

    let (status, fetched) = call(
        &app,
        get(&format!("/v1/deposit-addresses/{user_id}/{TEST_CHAIN}")),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(fetched["address"].as_str().unwrap(), assigned);
}

#[tokio::test]
async fn the_openapi_document_is_served() {
    let db = require_db!();
    let app = app(&db, None).await;
    let (status, body) = call(&app, get("/openapi.json")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["openapi"], "3.0.3");
    assert!(body["paths"]["/v1/withdrawals"]["post"].is_object());
}

#[tokio::test]
async fn unknown_chains_and_assets_are_400_not_500() {
    let db = require_db!();
    let f = fixture(&db).await;
    let app = app(&db, None).await;
    fund(&db, f.user_id, f.asset_id, 100_000_000).await;

    let (status, body) = call(
        &app,
        post(
            "/v1/withdrawals",
            json!({
                "user_id": f.user_id,
                "chain": "dogecoin",
                "asset": "USDC",
                "amount": "1000000",
                "destination": "0x70997970C51812dc3A010C7d01b50e0d17dc79C8",
                "idempotency_key": "bad-chain-0001"
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["code"], "unsupported_chain");
}
