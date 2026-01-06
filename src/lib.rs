//! Sustainalytics WASM FDW for Supabase Wrappers
//!
//! Endpoints:
//! - POST /auth/token
//! - GET /v2/DataService             (table option endpoint: 'DataServices')
//! - GET /v2/FieldMappingDefinitions (table option endpoint: 'FieldMappingDefinitions')
//!
//! Notes:
//! - Token is cached in-memory for best-effort performance.
//! - If a request returns 401/403, the token is refreshed once and retried.
//! - DataServices supports paging via Skip/Take.
//! - Take defaults to 10 and clamps to <= 10 (or uses <10 if provided, per your requested rule).
//!
//! IMPORTANT:
//! - This version reads `client_id` and `client_secret` from **server options**.

use serde::Deserialize;
use serde_json::Value as JsonValue;
use urlencoding::encode;
use supabase_wrappers::prelude::*;

const DEFAULT_BASE_URL: &str = "https://api.sustainalytics.com";
const DEFAULT_TAKE: i64 = 10;
const MAX_TAKE: i64 = 10;

#[derive(Default, Clone)]
struct SustainalyticsFdw {
    base_url: String,
    client_id: String,
    client_secret: String,
    cached_token: Option<String>,
    scan: ScanState,
}

#[derive(Default, Clone)]
enum ScanState {
    #[default]
    None,
    DataServices(DataServicesScan),
    FieldMappingDefinitions(FieldMappingDefinitionsScan),
}

#[derive(Default, Clone)]
struct DataServicesScan {
    params: DataServicesParams,
    skip: i64,
    page_rows: Vec<JsonValue>,
    page_idx: usize,
    done: bool,
}

#[derive(Default, Clone)]
struct DataServicesParams {
    product_id: String,
    package_ids: Option<String>,
    field_cluster_ids: Option<String>,
    field_ids: Option<String>,
    take: i64,
}

#[derive(Default, Clone)]
struct FieldMappingDefinitionsScan {
    rows: Vec<FieldMappingRow>,
    idx: usize,
}

#[derive(Debug, Clone)]
struct FieldMappingRow {
    product_id: String,
    product_name: Option<String>,
    package_id: Option<i64>,
    package_name: Option<String>,
    field_cluster_id: Option<i64>,
    field_cluster_name: Option<String>,
    field_id: Option<i64>,
    field_name: Option<String>,
    description: Option<String>,
    field_type: Option<String>,
    field_length: Option<String>,
    possible_values: Option<String>,
    grouping: Option<String>,
    parentage: Option<JsonValue>,
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
    expires_in: i64,
    token_type: String,
    scope: Option<String>,
}

impl SustainalyticsFdw {
    fn normalize_take(raw: Option<String>) -> i64 {
        let Some(s) = raw else { return DEFAULT_TAKE; };
        let Ok(n) = s.parse::<i64>() else { return DEFAULT_TAKE; };
        if n < 1 { return DEFAULT_TAKE; }
        if n < MAX_TAKE { n } else { MAX_TAKE }
    }

    fn build_dataservices_url(&self, p: &DataServicesParams, skip: i64) -> String {
        let base = self.base_url.trim_end_matches('/');

        let mut parts: Vec<String> = vec![
            format!("ProductId={}", encode(&p.product_id)),
            format!("Skip={}", skip),
            format!("Take={}", p.take),
        ];

        if let Some(v) = &p.package_ids {
            parts.push(format!("PackageIds={}", encode(v)));
        }
        if let Some(v) = &p.field_cluster_ids {
            parts.push(format!("FieldClusterIds={}", encode(v)));
        }
        if let Some(v) = &p.field_ids {
            parts.push(format!("FieldIds={}", encode(v)));
        }

        format!("{}/v2/DataService?{}", base, parts.join("&"))
    }

    fn fetch_token(&mut self) -> FdwResult<String> {
        let url = format!("{}/auth/token", self.base_url.trim_end_matches('/'));

        let body = format!(
            "client_id={}&client_secret={}",
            encode(&self.client_id),
            encode(&self.client_secret),
        );

        let req = http::Request {
            method: http::Method::Post,
            url,
            headers: vec![
                ("content-type".to_owned(), "application/x-www-form-urlencoded".to_owned()),
                ("accept".to_owned(), "application/json".to_owned()),
            ],
            body,
        };

        let resp = http::post(&req)?;
        if !(200..300).contains(&resp.status_code) {
            return Err(format!("auth failed: status={} body={}", resp.status_code, resp.body).into());
        }

        let tr: TokenResponse = serde_json::from_str(&resp.body)
            .map_err(|e| format!("invalid auth json: {e}"))?;

        self.cached_token = Some(tr.access_token.clone());
        Ok(tr.access_token)
    }

    fn ensure_token(&mut self) -> FdwResult<String> {
        if let Some(tok) = &self.cached_token {
            return Ok(tok.clone());
        }
        self.fetch_token()
    }

    fn get_json_with_bearer(&mut self, url: &str) -> FdwResult<(i32, JsonValue)> {
        let token = self.ensure_token()?;
        let req = http::Request {
            method: http::Method::Get,
            url: url.to_string(),
            headers: vec![
                ("accept".to_owned(), "application/json".to_owned()),
                ("authorization".to_owned(), format!("Bearer {}", token)),
            ],
            body: String::new(),
        };

        let resp = http::get(&req)?;
        let status = resp.status_code;

        if status == 401 || status == 403 {
            let _ = self.fetch_token()?;
            let token2 = self.ensure_token()?;
            let req2 = http::Request {
                method: http::Method::Get,
                url: url.to_string(),
                headers: vec![
                    ("accept".to_owned(), "application/json".to_owned()),
                    ("authorization".to_owned(), format!("Bearer {}", token2)),
                ],
                body: String::new(),
            };
            let resp2 = http::get(&req2)?;
            let v2: JsonValue = serde_json::from_str(&resp2.body)
                .map_err(|e| format!("invalid json: {e}"))?;
            return Ok((resp2.status_code, v2));
        }

        let v: JsonValue = serde_json::from_str(&resp.body)
            .map_err(|e| format!("invalid json: {e}"))?;
        Ok((status, v))
    }

    fn load_dataservices_page(&mut self, scan: &mut DataServicesScan) -> FdwResult<()> {
        if scan.done {
            return Ok(());
        }

        let url = self.build_dataservices_url(&scan.params, scan.skip);
        let (status, json) = self.get_json_with_bearer(&url)?;
        if !(200..300).contains(&status) {
            return Err(format!("DataServices failed: status={} url={}", status, url).into());
        }

        let arr = json.as_array().ok_or("DataServices response not an array")?.to_vec();
        scan.page_rows = arr;
        scan.page_idx = 0;

        if (scan.page_rows.len() as i64) < scan.params.take {
            scan.done = true;
        } else {
            scan.skip += scan.params.take;
        }

        Ok(())
    }

    fn ensure_dataservices_rows(&mut self, scan: &mut DataServicesScan) -> FdwResult<()> {
        if scan.page_idx < scan.page_rows.len() {
            return Ok(());
        }
        if scan.done {
            return Ok(());
        }
        self.load_dataservices_page(scan)
    }

    fn load_field_mapping_definitions(&mut self) -> FdwResult<Vec<FieldMappingRow>> {
        let url = format!("{}/v2/FieldMappingDefinitions", self.base_url.trim_end_matches('/'));
        let (status, json) = self.get_json_with_bearer(&url)?;
        if !(200..300).contains(&status) {
            return Err(format!("FieldMappingDefinitions failed: status={} body={}", status, json).into());
        }

        let products = json.as_array().ok_or("FieldMappingDefinitions not an array")?;
        let mut out: Vec<FieldMappingRow> = Vec::new();

        for prod in products {
            let product_id = prod.get("productId")
                .map(|v| v.to_string().trim_matches('\"').to_string())
                .unwrap_or_default();
            let product_name = prod.get("productName").and_then(|v| v.as_str()).map(|s| s.to_string());

            let packages = prod.get("packages").and_then(|v| v.as_array()).cloned().unwrap_or_default();
            for pkg in packages {
                let package_id = pkg.get("packageId").and_then(|v| v.as_i64());
                let package_name = pkg.get("packageName").and_then(|v| v.as_str()).map(|s| s.to_string());

                let clusters = pkg.get("clusters").and_then(|v| v.as_array()).cloned().unwrap_or_default();
                for cl in clusters {
                    let field_cluster_id = cl.get("fieldClusterId").and_then(|v| v.as_i64());
                    let field_cluster_name = cl.get("fieldClusterName").and_then(|v| v.as_str()).map(|s| s.to_string());

                    let defs = cl.get("fieldDefinitions").and_then(|v| v.as_array()).cloned().unwrap_or_default();
                    for d in defs {
                        out.push(FieldMappingRow {
                            product_id: product_id.clone(),
                            product_name: product_name.clone(),
                            package_id,
                            package_name: package_name.clone(),
                            field_cluster_id,
                            field_cluster_name: field_cluster_name.clone(),
                            field_id: d.get("fieldId").and_then(|v| v.as_i64()),
                            field_name: d.get("fieldName").and_then(|v| v.as_str()).map(|s| s.to_string()),
                            description: d.get("description").and_then(|v| v.as_str()).map(|s| s.to_string()),
                            field_type: d.get("fieldType").and_then(|v| v.as_str()).map(|s| s.to_string()),
                            field_length: d.get("fieldLength").and_then(|v| v.as_str()).map(|s| s.to_string()),
                            possible_values: d.get("possibleValues").and_then(|v| v.as_str()).map(|s| s.to_string()),
                            grouping: d.get("grouping").and_then(|v| v.as_str()).map(|s| s.to_string()),
                            parentage: d.get("parentage").cloned(),
                        });
                    }
                }
            }
        }

        Ok(out)
    }
}

static INSTANCE: std::sync::Mutex<Option<SustainalyticsFdw>> = std::sync::Mutex::new(None);

#[supabase_wrappers::fdw]
impl ForeignDataWrapper for SustainalyticsFdw {
    fn init(ctx: &Context) -> FdwResult<()> {
        let sopts = ctx.get_options(OptionsType::Server);

        let base_url = sopts.get("base_url").unwrap_or(DEFAULT_BASE_URL).to_string();
        let client_id = sopts.get("client_id").ok_or("missing server option client_id")?.to_string();
        let client_secret = sopts.get("client_secret").ok_or("missing server option client_secret")?.to_string();

        let fdw = SustainalyticsFdw {
            base_url,
            client_id,
            client_secret,
            cached_token: None,
            scan: ScanState::None,
        };

        *INSTANCE.lock().unwrap() = Some(fdw);
        Ok(())
    }

    fn begin_scan(ctx: &Context) -> FdwResult<()> {
        let mut guard = INSTANCE.lock().unwrap();
        let fdw = guard.as_mut().ok_or("FDW not initialized")?;

        let topts = ctx.get_options(OptionsType::Table);
        let endpoint = topts.get("endpoint").ok_or("missing table option endpoint")?.to_string();

        match endpoint.as_str() {
            "DataServices" => {
                let allowed = ["endpoint", "ProductId", "PackageIds", "FieldClusterIds", "FieldIds", "Take"];
                for (k, _) in topts.iter() {
                    if !allowed.contains(&k.as_str()) {
                        return Err(format!("unsupported table option for DataServices: {}", k).into());
                    }
                }

                let product_id = topts.get("ProductId").ok_or("missing required table option ProductId")?.to_string();
                let take = SustainalyticsFdw::normalize_take(topts.get("Take").map(|s| s.to_string()));

                let params = DataServicesParams {
                    product_id,
                    package_ids: topts.get("PackageIds").map(|s| s.to_string()),
                    field_cluster_ids: topts.get("FieldClusterIds").map(|s| s.to_string()),
                    field_ids: topts.get("FieldIds").map(|s| s.to_string()),
                    take,
                };

                let mut scan = DataServicesScan {
                    params,
                    skip: 0,
                    page_rows: vec![],
                    page_idx: 0,
                    done: false,
                };

                fdw.load_dataservices_page(&mut scan)?;
                fdw.scan = ScanState::DataServices(scan);
                Ok(())
            }

            "FieldMappingDefinitions" => {
                let rows = fdw.load_field_mapping_definitions()?;
                fdw.scan = ScanState::FieldMappingDefinitions(FieldMappingDefinitionsScan { rows, idx: 0 });
                Ok(())
            }

            other => Err(format!("unknown endpoint: {}", other).into()),
        }
    }

    fn iter_scan(ctx: &Context, row: &Row) -> Result<Option<u32>, FdwError> {
        let mut guard = INSTANCE.lock().unwrap();
        let fdw = guard.as_mut().ok_or("FDW not initialized")?;

        match &mut fdw.scan {
            ScanState::DataServices(scan) => {
                fdw.ensure_dataservices_rows(scan)?;

                if scan.page_idx >= scan.page_rows.len() {
                    return Ok(None);
                }

                let src = &scan.page_rows[scan.page_idx];

                for col in ctx.get_columns() {
                    let cell = match col.name() {
                        "entityId" => src.get("entityId").map(|v| Cell::String(v.to_string().trim_matches('\"').to_string())),
                        "entityName" => src.get("entityName").and_then(|v| v.as_str().map(|s| Cell::String(s.to_string()))),
                        "fields" => src.get("fields").map(|v| Cell::Jsonb(v.to_string())),
                        other => return Err(format!("unsupported column for DataServices: {}", other).into()),
                    };
                    row.push(cell.as_ref());
                }

                scan.page_idx += 1;
                Ok(Some(0))
            }

            ScanState::FieldMappingDefinitions(scan) => {
                if scan.idx >= scan.rows.len() {
                    return Ok(None);
                }

                let r = &scan.rows[scan.idx];

                for col in ctx.get_columns() {
                    let cell = match col.name() {
                        "product_id" => Some(Cell::String(r.product_id.clone())),
                        "product_name" => r.product_name.clone().map(Cell::String),
                        "package_id" => r.package_id.map(Cell::I64),
                        "package_name" => r.package_name.clone().map(Cell::String),
                        "field_cluster_id" => r.field_cluster_id.map(Cell::I64),
                        "field_cluster_name" => r.field_cluster_name.clone().map(Cell::String),
                        "field_id" => r.field_id.map(Cell::I64),
                        "field_name" => r.field_name.clone().map(Cell::String),
                        "description" => r.description.clone().map(Cell::String),
                        "field_type" => r.field_type.clone().map(Cell::String),
                        "field_length" => r.field_length.clone().map(Cell::String),
                        "possible_values" => r.possible_values.clone().map(Cell::String),
                        "grouping" => r.grouping.clone().map(Cell::String),
                        "parentage" => r.parentage.clone().map(|v| Cell::Jsonb(v.to_string())),
                        other => return Err(format!("unsupported column for FieldMappingDefinitions: {}", other).into()),
                    };
                    row.push(cell.as_ref());
                }

                scan.idx += 1;
                Ok(Some(0))
            }

            ScanState::None => Ok(None),
        }
    }

    fn end_scan(_ctx: &Context) -> FdwResult<()> {
        let mut guard = INSTANCE.lock().unwrap();
        if let Some(fdw) = guard.as_mut() {
            fdw.scan = ScanState::None;
        }
        Ok(())
    }
}
