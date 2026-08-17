//! Route handlers.

use std::str::FromStr;
use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::{Extension, Json};
use chainrail_common::{Address, ChainKind, Error, Hash32, Result};
use chainrail_database::models::{DepositStatus, WithdrawalStatus};
use chainrail_database::repo;
use uuid::Uuid;

use crate::dto::*;
use crate::error::ApiError;
use crate::middleware::RequestContext;
use crate::state::AppState;

type ApiResult<T> = std::result::Result<T, ApiError>;

/// Bridge domain errors into HTTP responses, attaching the request id.
trait IntoApi<T> {
    fn api(self, ctx: &RequestContext) -> ApiResult<T>;
}

impl<T> IntoApi<T> for Result<T> {
    fn api(self, ctx: &RequestContext) -> ApiResult<T> {
        self.map_err(|e| ApiError::new(e, ctx.request_id.clone()))
    }
}

// ------------------------------------------------------------------- users ---

pub async fn create_user(
    State(state): State<Arc<AppState>>,
    Extension(ctx): Extension<RequestContext>,
    ValidJson(body): ValidJson<CreateUserRequest>,
) -> ApiResult<impl IntoResponse> {
    let external_id = body.external_id.trim();
    if external_id.is_empty() || external_id.len() > 128 {
        return Err(ApiError::new(
            Error::Validation("external_id must be 1..=128 characters".into()),
            ctx.request_id,
        ));
    }

    // Idempotent: the same external_id returns the existing user rather than
    // erroring, so a client retry is safe.
    let existed = repo::reference::get_user_by_external_id(state.db.pool(), external_id)
        .await
        .api(&ctx)?
        .is_some();
    let user = repo::reference::create_user(state.db.pool(), external_id)
        .await
        .api(&ctx)?;

    let status = if existed {
        StatusCode::OK
    } else {
        StatusCode::CREATED
    };
    Ok((status, Json(UserResponse::from(user))))
}

pub async fn get_user(
    State(state): State<Arc<AppState>>,
    Extension(ctx): Extension<RequestContext>,
    Path(user_id): Path<Uuid>,
) -> ApiResult<Json<UserResponse>> {
    let user = repo::reference::get_user(state.db.pool(), user_id)
        .await
        .api(&ctx)?;
    Ok(Json(UserResponse::from(user)))
}

// -------------------------------------------------------- deposit addresses ---

pub async fn create_deposit_address(
    State(state): State<Arc<AppState>>,
    Extension(ctx): Extension<RequestContext>,
    ValidJson(body): ValidJson<CreateDepositAddressRequest>,
) -> ApiResult<impl IntoResponse> {
    let chain_cfg = state
        .cfg
        .chain(&body.chain)
        .ok_or_else(|| Error::UnsupportedChain(body.chain.clone()))
        .api(&ctx)?;
    // Fail if the user does not exist, rather than creating an orphan address.
    repo::reference::get_user(state.db.pool(), body.user_id)
        .await
        .api(&ctx)?;

    if let Some(existing) =
        repo::reference::get_deposit_address(state.db.pool(), body.user_id, &body.chain)
            .await
            .api(&ctx)?
    {
        let checksummed = checksum(&existing.address, chain_cfg.kind);
        return Ok((
            StatusCode::OK,
            Json(DepositAddressResponse::new(existing, checksummed)),
        ));
    }

    // ChainRail does not derive addresses: that would require a master key in
    // this process, which the threat model rules out. Addresses are exported
    // from the custody system into `deposit_address_pool`, and assignment claims
    // the next free one atomically.
    let mut tx = state.db.begin().await.api(&ctx)?;
    let assigned = repo::reference::claim_pool_address(&mut tx, body.user_id, &body.chain)
        .await
        .api(&ctx)?;
    tx.commit()
        .await
        .map_err(chainrail_database::map_sqlx)
        .api(&ctx)?;

    let checksummed = checksum(&assigned.address, chain_cfg.kind);
    tracing::info!(
        user_id = %body.user_id, chain = %body.chain, address = %checksummed,
        "deposit address assigned from the pool"
    );
    Ok((
        StatusCode::CREATED,
        Json(DepositAddressResponse::new(assigned, checksummed)),
    ))
}

pub async fn get_deposit_address(
    State(state): State<Arc<AppState>>,
    Extension(ctx): Extension<RequestContext>,
    Path((user_id, chain)): Path<(Uuid, String)>,
) -> ApiResult<Json<DepositAddressResponse>> {
    let chain_cfg = state
        .cfg
        .chain(&chain)
        .ok_or_else(|| Error::UnsupportedChain(chain.clone()))
        .api(&ctx)?;
    let addr = repo::reference::get_deposit_address(state.db.pool(), user_id, &chain)
        .await
        .api(&ctx)?
        .ok_or(Error::NotFound {
            entity: "deposit address",
        })
        .api(&ctx)?;
    let checksummed = checksum(&addr.address, chain_cfg.kind);
    Ok(Json(DepositAddressResponse::new(addr, checksummed)))
}

// ---------------------------------------------------------------- deposits ---

pub async fn list_deposits(
    State(state): State<Arc<AppState>>,
    Extension(ctx): Extension<RequestContext>,
    Query(filter): Query<DepositFilter>,
) -> ApiResult<Json<Page<DepositResponse>>> {
    let status = filter
        .status
        .as_deref()
        .map(DepositStatus::from_str)
        .transpose()
        .api(&ctx)?;
    let limit = state.page_size(filter.page.limit);

    let deposits = repo::deposits::list_deposits(
        state.db.pool(),
        filter.user_id,
        status,
        filter.page.cursor(),
        limit + 1, // one extra to detect a further page
    )
    .await
    .api(&ctx)?;

    let has_more = deposits.len() as i64 > limit;
    let deposits: Vec<_> = deposits.into_iter().take(limit as usize).collect();
    let next_cursor = if has_more {
        deposits.last().map(|d| Cursor {
            after: d.created_at,
            after_id: d.id,
        })
    } else {
        None
    };

    let mut items = Vec::with_capacity(deposits.len());
    for d in deposits {
        items.push(deposit_view(&state, d).await.api(&ctx)?.into());
    }
    Ok(Json(Page::new(items, next_cursor)))
}

pub async fn get_deposit(
    State(state): State<Arc<AppState>>,
    Extension(ctx): Extension<RequestContext>,
    Path(id): Path<Uuid>,
) -> ApiResult<Json<DepositResponse>> {
    let deposit = repo::deposits::get_deposit(state.db.pool(), id)
        .await
        .api(&ctx)?;
    Ok(Json(deposit_view(&state, deposit).await.api(&ctx)?.into()))
}

async fn deposit_view(
    state: &Arc<AppState>,
    deposit: chainrail_database::models::Deposit,
) -> Result<DepositView> {
    let asset = repo::reference::get_asset_by_id(state.db.pool(), deposit.asset_id).await?;
    let transfer = sqlx::query_as::<_, chainrail_database::models::BlockchainTransaction>(
        "SELECT id, chain, tx_hash, log_index, block_number, block_hash, from_address,
                to_address, asset_id, amount_raw, status, observed_at
           FROM blockchain_transactions WHERE id = $1",
    )
    .bind(deposit.blockchain_transaction_id)
    .fetch_optional(state.db.pool())
    .await
    .map_err(chainrail_database::map_sqlx)?;

    let required = state
        .cfg
        .chain(&asset.chain)
        .and_then(|c| c.finality.required_confirmations());

    let decimals = asset.decimals_u8();
    Ok(DepositView {
        asset_symbol: asset.symbol,
        asset_decimals: decimals,
        chain: asset.chain,
        tx_hash: transfer.as_ref().map(|t| t.tx_hash.to_string()),
        block_number: transfer.as_ref().map(|t| t.block_number),
        required_confirmations: required,
        deposit,
    })
}

// ------------------------------------------------------------ transactions ---

pub async fn get_transaction(
    State(state): State<Arc<AppState>>,
    Extension(ctx): Extension<RequestContext>,
    Path(hash): Path<String>,
) -> ApiResult<Json<TransactionResponse>> {
    let hash = Hash32::parse(&hash).api(&ctx)?;
    let transfers = repo::chain::get_transfer_by_hash(state.db.pool(), &hash)
        .await
        .api(&ctx)?;
    if transfers.is_empty() {
        return Err(ApiError::new(
            Error::NotFound {
                entity: "transaction",
            },
            ctx.request_id,
        ));
    }
    let chain = transfers[0].chain.clone();
    Ok(Json(TransactionResponse {
        tx_hash: hash.to_string(),
        chain,
        transfers: transfers.into_iter().map(Into::into).collect(),
    }))
}

// ---------------------------------------------------------------- balances ---

pub async fn get_balances(
    State(state): State<Arc<AppState>>,
    Extension(ctx): Extension<RequestContext>,
    Path(user_id): Path<Uuid>,
) -> ApiResult<Json<BalanceResponse>> {
    // 404 for an unknown user rather than an empty balance list, which would be
    // indistinguishable from a real user holding nothing.
    repo::reference::get_user(state.db.pool(), user_id)
        .await
        .api(&ctx)?;
    let rows = chainrail_ledger::get_balances(state.db.pool(), user_id)
        .await
        .api(&ctx)?;
    Ok(Json(build_balances(user_id, rows)))
}

pub async fn get_ledger(
    State(state): State<Arc<AppState>>,
    Extension(ctx): Extension<RequestContext>,
    Path(user_id): Path<Uuid>,
    Query(page): Query<Pagination>,
) -> ApiResult<Json<Page<LedgerEntryResponse>>> {
    repo::reference::get_user(state.db.pool(), user_id)
        .await
        .api(&ctx)?;
    let limit = state.page_size(page.limit);
    let entries =
        chainrail_ledger::get_ledger_entries(state.db.pool(), user_id, page.cursor(), limit + 1)
            .await
            .api(&ctx)?;

    let has_more = entries.len() as i64 > limit;
    let entries: Vec<_> = entries.into_iter().take(limit as usize).collect();
    let next_cursor = if has_more {
        entries.last().map(|e| Cursor {
            after: e.created_at,
            after_id: e.id,
        })
    } else {
        None
    };
    Ok(Json(Page::new(
        entries.into_iter().map(Into::into).collect(),
        next_cursor,
    )))
}

// ------------------------------------------------------------- withdrawals ---

pub async fn create_withdrawal(
    State(state): State<Arc<AppState>>,
    Extension(ctx): Extension<RequestContext>,
    ValidJson(body): ValidJson<CreateWithdrawalRequest>,
) -> ApiResult<impl IntoResponse> {
    let result = state
        .withdrawals
        .create(chainrail_withdrawals::WithdrawalRequest {
            user_id: body.user_id,
            chain: body.chain,
            asset_symbol: body.asset,
            amount: body.amount,
            destination: body.destination,
            idempotency_key: body.idempotency_key,
            correlation_id: Some(ctx.correlation_id.clone()),
        })
        .await
        .api(&ctx)?;

    // 201 for a new withdrawal, 200 for an idempotent replay, so a client can
    // tell whether its retry actually created anything.
    let status = if result.is_new() {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };
    let w = result.withdrawal().clone();
    let response = withdrawal_response(&state, w).await.api(&ctx)?;
    Ok((status, Json(response)))
}

pub async fn get_withdrawal(
    State(state): State<Arc<AppState>>,
    Extension(ctx): Extension<RequestContext>,
    Path(id): Path<Uuid>,
) -> ApiResult<Json<WithdrawalResponse>> {
    let w = repo::withdrawals::get_withdrawal(state.db.pool(), id)
        .await
        .api(&ctx)?;
    Ok(Json(withdrawal_response(&state, w).await.api(&ctx)?))
}

pub async fn list_withdrawals(
    State(state): State<Arc<AppState>>,
    Extension(ctx): Extension<RequestContext>,
    Query(filter): Query<WithdrawalFilter>,
) -> ApiResult<Json<Page<WithdrawalResponse>>> {
    let status = filter
        .status
        .as_deref()
        .map(WithdrawalStatus::from_str)
        .transpose()
        .api(&ctx)?;
    let limit = state.page_size(filter.page.limit);
    let rows = repo::withdrawals::list_withdrawals(
        state.db.pool(),
        filter.user_id,
        status,
        filter.page.cursor(),
        limit + 1,
    )
    .await
    .api(&ctx)?;

    let has_more = rows.len() as i64 > limit;
    let rows: Vec<_> = rows.into_iter().take(limit as usize).collect();
    let next_cursor = if has_more {
        rows.last().map(|w| Cursor {
            after: w.created_at,
            after_id: w.id,
        })
    } else {
        None
    };

    let mut items = Vec::with_capacity(rows.len());
    for w in rows {
        items.push(withdrawal_response(&state, w).await.api(&ctx)?);
    }
    Ok(Json(Page::new(items, next_cursor)))
}

pub async fn cancel_withdrawal(
    State(state): State<Arc<AppState>>,
    Extension(ctx): Extension<RequestContext>,
    Path(id): Path<Uuid>,
) -> ApiResult<Json<WithdrawalResponse>> {
    let w = state
        .withdrawals
        .cancel(id, "cancelled via API")
        .await
        .api(&ctx)?;
    Ok(Json(withdrawal_response(&state, w).await.api(&ctx)?))
}

pub async fn approve_withdrawal(
    State(state): State<Arc<AppState>>,
    Extension(ctx): Extension<RequestContext>,
    Path(id): Path<Uuid>,
) -> ApiResult<Json<WithdrawalResponse>> {
    let w = state.withdrawals.approve(id).await.api(&ctx)?;
    Ok(Json(withdrawal_response(&state, w).await.api(&ctx)?))
}

async fn withdrawal_response(
    state: &Arc<AppState>,
    w: chainrail_database::models::Withdrawal,
) -> Result<WithdrawalResponse> {
    let asset = repo::reference::get_asset_by_id(state.db.pool(), w.asset_id).await?;
    let kind = state
        .cfg
        .chain(&w.chain)
        .map(|c| c.kind)
        .unwrap_or(ChainKind::Evm);
    let dest = checksum(w.destination_address.as_str(), kind);
    let symbol = asset.symbol.clone();
    let decimals = asset.decimals_u8();
    Ok(WithdrawalResponse::new(w, symbol, decimals, dest))
}

// ------------------------------------------------------------- operational ---

pub async fn health(State(state): State<Arc<AppState>>) -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok",
        service: state.cfg.service_name.clone(),
        version: env!("CARGO_PKG_VERSION"),
        environment: state.cfg.environment.clone(),
        uptime_seconds: state.started_at.elapsed().as_secs(),
        signer_backend: state.signer.backend_name(),
        signer_production_grade: state.signer.is_production_grade(),
    })
}

pub async fn ready(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let readiness = state.readiness().await;
    let status = if readiness.ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (status, Json(readiness))
}

pub async fn metrics(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    match &state.metrics {
        Some(m) => (
            StatusCode::OK,
            [("content-type", "text/plain; version=0.0.4")],
            m.render(),
        ),
        None => (
            StatusCode::SERVICE_UNAVAILABLE,
            [("content-type", "text/plain")],
            "metrics recorder not installed".to_string(),
        ),
    }
}

/// Ledger integrity report. An operational endpoint: it scans the whole ledger,
/// so it is deliberately not on any user-facing path.
pub async fn ledger_integrity(
    State(state): State<Arc<AppState>>,
    Extension(ctx): Extension<RequestContext>,
) -> ApiResult<impl IntoResponse> {
    let report = chainrail_ledger::verify_ledger_integrity(state.db.pool())
        .await
        .api(&ctx)?;
    let status = if report.is_clean() {
        StatusCode::OK
    } else {
        // A non-clean ledger is a genuine emergency, and a 500 makes monitoring
        // notice even if nobody reads the body.
        StatusCode::INTERNAL_SERVER_ERROR
    };
    Ok((status, Json(report)))
}

/// Render an address in its canonical display form.
fn checksum(stored_lowercase: &str, kind: ChainKind) -> String {
    match Address::parse(kind, stored_lowercase) {
        Ok(a) => a.to_string(),
        // Already-stored values are trusted; if re-parsing fails (e.g. the zero
        // address, which `parse` rejects) fall back to what we hold.
        Err(_) => stored_lowercase.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checksums_are_applied_for_display() {
        let lower = "0x5aaeb6053f3e94c9b9a09f33669435e7ef1beaed";
        assert_eq!(
            checksum(lower, ChainKind::Evm),
            "0x5aAeb6053F3E94C9b9A09f33669435E7Ef1BeAed"
        );
    }

    #[test]
    fn unparseable_stored_addresses_fall_back_rather_than_panicking() {
        // The zero address is rejected as a destination but can legitimately
        // appear as a transfer's `from`.
        let zero = "0x0000000000000000000000000000000000000000";
        assert_eq!(checksum(zero, ChainKind::Evm), zero);
        assert_eq!(checksum("not-an-address", ChainKind::Evm), "not-an-address");
    }

    #[test]
    fn status_filters_reject_unknown_values() {
        assert!(DepositStatus::from_str("credited").is_ok());
        assert!(DepositStatus::from_str("bogus").is_err());
        assert!(WithdrawalStatus::from_str("broadcast").is_ok());
        assert!(WithdrawalStatus::from_str("bogus").is_err());
    }
}
