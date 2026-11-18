use axum::{
    body::Bytes,
    extract::{Path, Query, State},
    http::{header, HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Router,
};
use parking_lot::RwLock;
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use sqlx::{postgres::PgPoolOptions, PgPool, Row};
use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
    env,
    sync::{
        atomic::{AtomicI32, AtomicU64, Ordering as AtomicOrdering},
        Arc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::net::TcpListener;

// --- Zero-Allocation Views & Models ---

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ProductResponse {
    pub id: String,
    pub name: String,
    pub category: String,
    pub price: i64,
    pub stock: i32,
}

#[derive(Debug)]
pub struct ProductItem {
    pub id: String,
    pub name: String,
    pub category: String,
    pub price: i64,
    pub stock: AtomicI32,
}

#[derive(Debug, Serialize)]
pub struct ProductView<'a> {
    pub id: &'a str,
    pub name: &'a str,
    pub category: &'a str,
    pub price: i64,
    pub stock: i32,
}

impl ProductItem {
    #[inline(always)]
    fn as_view(&self) -> ProductView<'_> {
        ProductView {
            id: &self.id,
            name: &self.name,
            category: &self.category,
            price: self.price,
            stock: self.stock.load(AtomicOrdering::Relaxed).max(0),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: &'static str,
}

impl ErrorResponse {
    #[inline(always)]
    pub fn new(error: &'static str) -> Self {
        Self { error }
    }
}

#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub status: &'static str,
}

#[derive(Debug, Serialize)]
pub struct CategoryPageResponse<'a> {
    pub items: Vec<ProductView<'a>>,
    pub page: i64,
    pub limit: i64,
    pub total: usize,
}

#[derive(Debug, Serialize, Clone)]
pub struct UserOrderItem {
    #[serde(rename = "orderId")]
    pub order_id: String,
    #[serde(rename = "productId")]
    pub product_id: String,
}

#[derive(Debug, Serialize)]
pub struct UserOrdersResponse {
    #[serde(rename = "userId")]
    pub user_id: String,
    pub orders: Vec<UserOrderItem>,
}

#[derive(Debug, Serialize)]
pub struct OrderCreatedResponse {
    #[serde(rename = "orderId")]
    pub order_id: String,
    #[serde(rename = "userId")]
    pub user_id: String,
    #[serde(rename = "productId")]
    pub product_id: String,
}

// --- App State with Split Fine-Grained Locks ---
// Splitting locks eliminates cross-thread lock contention:
// 60% of traffic (product reads) never blocks on 20% of traffic (order writes).

#[derive(Clone)]
pub struct AppState {
    pub db: PgPool,
    pub redis: redis::aio::ConnectionManager,
    pub products: Arc<RwLock<HashMap<String, Arc<ProductItem>>>>,
    pub categories: Arc<RwLock<HashMap<String, Vec<Arc<ProductItem>>>>>,
    pub user_orders: Arc<RwLock<HashMap<String, Vec<UserOrderItem>>>>,
    pub user_purchases: Arc<RwLock<HashSet<(String, String)>>>,
    pub order_counter: Arc<AtomicU64>,
}

#[inline(always)]
pub fn json_response<T: Serialize>(status: StatusCode, data: &T) -> Response {
    let json_bytes = match serde_json::to_vec(data) {
        Ok(b) => b,
        Err(_) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                [(header::CONTENT_TYPE, "application/json")],
                r#"{"error":"INTERNAL_ERROR"}"#,
            )
                .into_response();
        }
    };

    let mut headers = HeaderMap::new();
    headers.insert(header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
    (status, headers, json_bytes).into_response()
}

#[inline(always)]
pub fn json_error(status: StatusCode, code: &'static str) -> Response {
    json_response(status, &ErrorResponse::new(code))
}

// --- Endpoints ---

// GET /health
pub async fn health() -> Response {
    json_response(StatusCode::OK, &HealthResponse { status: "ok" })
}

// POST /products
pub async fn create_product(
    State(state): State<AppState>,
    body: Bytes,
) -> Response {
    let value: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(_) => return json_error(StatusCode::BAD_REQUEST, "VALIDATION_ERROR"),
    };

    let id = match value.get("id").and_then(|v| v.as_str()) {
        Some(s) if !s.is_empty() && s.len() <= 64 => s.to_string(),
        _ => return json_error(StatusCode::BAD_REQUEST, "VALIDATION_ERROR"),
    };

    let name = match value.get("name").and_then(|v| v.as_str()) {
        Some(s) if !s.is_empty() && s.len() <= 200 => s.to_string(),
        _ => return json_error(StatusCode::BAD_REQUEST, "VALIDATION_ERROR"),
    };

    let category = match value.get("category").and_then(|v| v.as_str()) {
        Some(s) if !s.is_empty() && s.len() <= 64 => s.to_string(),
        _ => return json_error(StatusCode::BAD_REQUEST, "VALIDATION_ERROR"),
    };

    let price = match value.get("price").and_then(|v| v.as_i64()) {
        Some(p) if p >= 0 => p,
        _ => return json_error(StatusCode::BAD_REQUEST, "VALIDATION_ERROR"),
    };

    let stock = match value.get("stock").and_then(|v| v.as_i64()) {
        Some(s) if s >= 0 && s <= i32::MAX as i64 => s as i32,
        _ => return json_error(StatusCode::BAD_REQUEST, "VALIDATION_ERROR"),
    };

    // Fast-path in-memory check
    {
        if state.products.read().contains_key(&id) {
            return json_error(StatusCode::CONFLICT, "PRODUCT_EXISTS");
        }
    }

    // Persist to PostgreSQL (authoritative source of truth)
    let insert_result = sqlx::query(
        "INSERT INTO products (id, name, category, price, stock) VALUES ($1, $2, $3, $4, $5)"
    )
    .bind(&id)
    .bind(&name)
    .bind(&category)
    .bind(price)
    .bind(stock)
    .execute(&state.db)
    .await;

    if let Err(e) = insert_result {
        if let Some(db_err) = e.as_database_error() {
            if db_err.is_unique_violation() {
                return json_error(StatusCode::CONFLICT, "PRODUCT_EXISTS");
            }
        }
        return json_error(StatusCode::INTERNAL_SERVER_ERROR, "DATABASE_ERROR");
    }

    let product_item = Arc::new(ProductItem {
        id: id.clone(),
        name: name.clone(),
        category: category.clone(),
        price,
        stock: AtomicI32::new(stock),
    });

    // Update in-memory catalogue and categories
    {
        state.products.write().insert(id.clone(), Arc::clone(&product_item));
        let mut cats = state.categories.write();
        let list = cats.entry(category.clone()).or_default();
        list.push(Arc::clone(&product_item));
        list.sort_by(|a, b| {
            let p = a.price.cmp(&b.price);
            if p == Ordering::Equal {
                a.id.cmp(&b.id)
            } else {
                p
            }
        });
    }

    // Populate Redis asynchronously
    let mut redis_conn = state.redis.clone();
    let redis_id = id.clone();
    let redis_json = serde_json::to_string(&product_item.as_view()).unwrap_or_default();
    tokio::spawn(async move {
        let key = format!("product:{redis_id}");
        let _: Result<(), _> = redis_conn.set_ex(key, redis_json, 60).await;
    });

    json_response(StatusCode::CREATED, &product_item.as_view())
}

// GET /products/:id (60% of all traffic)
pub async fn get_product(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Response {
    // Ultra-fast path: read from in-memory catalogue with zero string allocations
    if let Some(item) = {
        state.products.read().get(&id).cloned()
    } {
        return json_response(StatusCode::OK, &item.as_view());
    }

    // Fallback path: query PostgreSQL if missing from memory (e.g. after fresh restart)
    let query_result = sqlx::query(
        "SELECT id, name, category, price, stock FROM products WHERE id = $1"
    )
    .bind(&id)
    .fetch_optional(&state.db)
    .await;

    match query_result {
        Ok(Some(row)) => {
            let item_id: String = row.get("id");
            let item_name: String = row.get("name");
            let item_category: String = row.get("category");
            let item_price: i64 = row.get("price");
            let item_stock: i32 = row.get("stock");

            let item = Arc::new(ProductItem {
                id: item_id.clone(),
                name: item_name,
                category: item_category,
                price: item_price,
                stock: AtomicI32::new(item_stock),
            });
            let view = item.as_view();
            state.products.write().insert(item_id, Arc::clone(&item));
            json_response(StatusCode::OK, &view)
        }
        Ok(None) => json_error(StatusCode::NOT_FOUND, "PRODUCT_NOT_FOUND"),
        Err(_) => json_error(StatusCode::INTERNAL_SERVER_ERROR, "DATABASE_ERROR"),
    }
}

// GET /products?category=...&page=...&limit=... (15% of all traffic)
pub async fn list_products(
    State(state): State<AppState>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let category = match params.get("category") {
        Some(c) if !c.is_empty() => c,
        _ => return json_error(StatusCode::BAD_REQUEST, "VALIDATION_ERROR"),
    };

    let page: i64 = match params.get("page") {
        Some(p) => match p.parse() {
            Ok(n) if n >= 1 => n,
            _ => return json_error(StatusCode::BAD_REQUEST, "VALIDATION_ERROR"),
        },
        None => 1,
    };

    let limit: i64 = match params.get("limit") {
        Some(l) => match l.parse() {
            Ok(n) if (1..=50).contains(&n) => n,
            _ => return json_error(StatusCode::BAD_REQUEST, "VALIDATION_ERROR"),
        },
        None => 20,
    };

    let cats = state.categories.read();
    let (items, total) = if let Some(list) = cats.get(category) {
        let total = list.len();
        let offset = ((page - 1) * limit) as usize;
        let page_items = if offset < total {
            let end = (offset + limit as usize).min(total);
            list[offset..end].iter().map(|item| item.as_view()).collect()
        } else {
            Vec::new()
        };
        (page_items, total)
    } else {
        (Vec::new(), 0)
    };

    json_response(
        StatusCode::OK,
        &CategoryPageResponse {
            items,
            page,
            limit,
            total,
        },
    )
}

// POST /orders (20% of traffic)
pub async fn create_order(
    State(state): State<AppState>,
    body: Bytes,
) -> Response {
    let value: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(_) => return json_error(StatusCode::BAD_REQUEST, "VALIDATION_ERROR"),
    };

    let user_id = match value.get("userId").and_then(|v| v.as_str()) {
        Some(s) if !s.is_empty() && s.len() <= 64 => s.to_string(),
        _ => return json_error(StatusCode::BAD_REQUEST, "VALIDATION_ERROR"),
    };

    let product_id = match value.get("productId").and_then(|v| v.as_str()) {
        Some(s) if !s.is_empty() && s.len() <= 64 => s.to_string(),
        _ => return json_error(StatusCode::BAD_REQUEST, "VALIDATION_ERROR"),
    };

    // Fast-path in-memory checks:
    // 1. If user already purchased, reject immediately (retries)
    if state.user_purchases.read().contains(&(user_id.clone(), product_id.clone())) {
        return json_error(StatusCode::CONFLICT, "ALREADY_PURCHASED");
    }

    let maybe_item = state.products.read().get(&product_id).cloned();

    let item = match maybe_item {
        Some(i) => i,
        None => {
            // Verify with PostgreSQL
            let exists = sqlx::query("SELECT id FROM products WHERE id = $1")
                .bind(&product_id)
                .fetch_optional(&state.db)
                .await;
            match exists {
                Ok(Some(_)) => {
                    Arc::new(ProductItem {
                        id: product_id.clone(),
                        name: String::new(),
                        category: String::new(),
                        price: 0,
                        stock: AtomicI32::new(1),
                    })
                }
                Ok(None) => return json_error(StatusCode::NOT_FOUND, "PRODUCT_NOT_FOUND"),
                Err(_) => return json_error(StatusCode::INTERNAL_SERVER_ERROR, "DATABASE_ERROR"),
            }
        }
    };

    // Fast terminal sold-out guard:
    // When stock is 0 in a flash drop, immediately reject subsequent requests in RAM
    if item.stock.load(AtomicOrdering::Relaxed) <= 0 {
        return json_error(StatusCode::CONFLICT, "SOLD_OUT");
    }

    // Generate unique orderId
    let c = state.order_counter.fetch_add(1, AtomicOrdering::Relaxed);
    let t = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let order_id = format!("o_{:x}{:04x}", t, (c & 0xFFFF));

    // Execute atomic placement in PostgreSQL
    let query_result = sqlx::query_scalar::<_, i32>(
        "SELECT place_order($1, $2, $3)"
    )
    .bind(&order_id)
    .bind(&user_id)
    .bind(&product_id)
    .fetch_one(&state.db)
    .await;

    let status_code = match query_result {
        Ok(s) => s,
        _ => return json_error(StatusCode::INTERNAL_SERVER_ERROR, "DATABASE_ERROR"),
    };

    match status_code {
        0 => {
            // Success (201 Created)
            item.stock.fetch_sub(1, AtomicOrdering::Relaxed);

            let user_order = UserOrderItem {
                order_id: order_id.clone(),
                product_id: product_id.clone(),
            };

            state.user_purchases.write().insert((user_id.clone(), product_id.clone()));
            state.user_orders.write().entry(user_id.clone()).or_default().push(user_order);

            json_response(
                StatusCode::CREATED,
                &OrderCreatedResponse {
                    order_id,
                    user_id,
                    product_id,
                },
            )
        }
        1 => json_error(StatusCode::NOT_FOUND, "PRODUCT_NOT_FOUND"),
        2 => {
            state.user_purchases.write().insert((user_id, product_id));
            json_error(StatusCode::CONFLICT, "ALREADY_PURCHASED")
        }
        3 => {
            item.stock.store(0, AtomicOrdering::Relaxed);
            json_error(StatusCode::CONFLICT, "SOLD_OUT")
        }
        _ => json_error(StatusCode::INTERNAL_SERVER_ERROR, "DATABASE_ERROR"),
    }
}

// GET /users/:userId/orders (5% of traffic)
pub async fn get_user_orders(
    State(state): State<AppState>,
    Path(user_id): Path<String>,
) -> Response {
    // In-memory fast path
    if let Some(orders) = state.user_orders.read().get(&user_id) {
        return json_response(
            StatusCode::OK,
            &UserOrdersResponse {
                user_id,
                orders: orders.clone(),
            },
        );
    }

    // Query PostgreSQL
    let rows = sqlx::query(
        "SELECT order_id, product_id FROM orders WHERE user_id = $1"
    )
    .bind(&user_id)
    .fetch_all(&state.db)
    .await;

    match rows {
        Ok(list) => {
            let orders: Vec<UserOrderItem> = list
                .into_iter()
                .map(|r| UserOrderItem {
                    order_id: r.get("order_id"),
                    product_id: r.get("product_id"),
                })
                .collect();

            state.user_orders.write().insert(user_id.clone(), orders.clone());

            json_response(
                StatusCode::OK,
                &UserOrdersResponse { user_id, orders },
            )
        }
        Err(_) => json_error(StatusCode::INTERNAL_SERVER_ERROR, "DATABASE_ERROR"),
    }
}

// Fallback 404 for unhandled routes
pub async fn not_found_handler() -> Response {
    json_error(StatusCode::NOT_FOUND, "NOT_FOUND")
}

// --- Main ---

#[tokio::main]
async fn main() {
    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let redis_url = env::var("REDIS_URL").unwrap_or_else(|_| "redis://redis:6379".into());
    let port: u16 = env::var("PORT")
        .unwrap_or_else(|_| "8080".into())
        .parse()
        .expect("PORT must be a number");

    println!("Starting Last Server Standing API on port {port}...");

    // Configure high-performance PostgreSQL connection pool
    let db = PgPoolOptions::new()
        .max_connections(80)
        .min_connections(10)
        .acquire_timeout(Duration::from_secs(5))
        .idle_timeout(Duration::from_secs(300))
        .connect(&database_url)
        .await
        .expect("Failed to connect to PostgreSQL");

    // Initialize Schema & High-Throughput Function individually
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS products (
            id VARCHAR(64) PRIMARY KEY,
            name VARCHAR(200) NOT NULL,
            category VARCHAR(64) NOT NULL,
            price BIGINT NOT NULL,
            stock INT NOT NULL CHECK (stock >= 0)
        )",
    )
    .execute(&db)
    .await
    .expect("Failed to create products table");

    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_products_cat_price_id ON products(category, price ASC, id ASC)",
    )
    .execute(&db)
    .await
    .expect("Failed to create products index");

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS orders (
            order_id VARCHAR(64) PRIMARY KEY,
            user_id VARCHAR(64) NOT NULL,
            product_id VARCHAR(64) NOT NULL REFERENCES products(id),
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )",
    )
    .execute(&db)
    .await
    .expect("Failed to create orders table");

    sqlx::query(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_orders_user_product ON orders(user_id, product_id)",
    )
    .execute(&db)
    .await
    .expect("Failed to create orders user_product unique index");

    sqlx::query("CREATE INDEX IF NOT EXISTS idx_orders_user ON orders(user_id)")
        .execute(&db)
        .await
        .expect("Failed to create orders user index");

    sqlx::query(
        r#"
        CREATE OR REPLACE FUNCTION place_order(
            p_order_id TEXT,
            p_user_id TEXT,
            p_product_id TEXT
        ) RETURNS INT AS $$
        DECLARE
            v_updated_id TEXT;
        BEGIN
            -- 1. Fast check if user already purchased
            IF EXISTS (SELECT 1 FROM orders WHERE user_id = p_user_id AND product_id = p_product_id) THEN
                RETURN 2; -- ALREADY_PURCHASED (409)
            END IF;

            -- 2. Atomic decrement in single step
            UPDATE products
            SET stock = stock - 1
            WHERE id = p_product_id AND stock > 0
            RETURNING id INTO v_updated_id;

            IF v_updated_id IS NOT NULL THEN
                INSERT INTO orders (order_id, user_id, product_id)
                VALUES (p_order_id, p_user_id, p_product_id);
                RETURN 0; -- SUCCESS (201)
            END IF;

            -- Check if product exists or is sold out
            IF NOT EXISTS (SELECT 1 FROM products WHERE id = p_product_id) THEN
                RETURN 1; -- PRODUCT_NOT_FOUND (404)
            ELSE
                RETURN 3; -- SOLD_OUT (409)
            END IF;
        EXCEPTION
            WHEN unique_violation THEN
                -- Compensate stock decrement if concurrent duplicate order entered
                UPDATE products SET stock = stock + 1 WHERE id = p_product_id;
                RETURN 2; -- ALREADY_PURCHASED (409)
        END;
        $$ LANGUAGE plpgsql;
        "#,
    )
    .execute(&db)
    .await
    .expect("Failed to create place_order function");

    // Preload existing products into memory
    let mut products_map = HashMap::new();
    let mut categories_map: HashMap<String, Vec<Arc<ProductItem>>> = HashMap::new();

    let existing_products = sqlx::query("SELECT id, name, category, price, stock FROM products")
        .fetch_all(&db)
        .await
        .unwrap_or_default();

    for row in existing_products {
        let id: String = row.get("id");
        let name: String = row.get("name");
        let category: String = row.get("category");
        let price: i64 = row.get("price");
        let stock: i32 = row.get("stock");

        let item = Arc::new(ProductItem {
            id: id.clone(),
            name,
            category: category.clone(),
            price,
            stock: AtomicI32::new(stock),
        });
        products_map.insert(id, Arc::clone(&item));
        categories_map.entry(category).or_default().push(item);
    }

    for list in categories_map.values_mut() {
        list.sort_by(|a, b| {
            let p = a.price.cmp(&b.price);
            if p == Ordering::Equal {
                a.id.cmp(&b.id)
            } else {
                p
            }
        });
    }

    // Preload existing orders into memory
    let mut user_orders_map: HashMap<String, Vec<UserOrderItem>> = HashMap::new();
    let mut user_purchases_set = HashSet::new();

    let existing_orders = sqlx::query("SELECT user_id, product_id, order_id FROM orders")
        .fetch_all(&db)
        .await
        .unwrap_or_default();

    for row in existing_orders {
        let user_id: String = row.get("user_id");
        let product_id: String = row.get("product_id");
        let order_id: String = row.get("order_id");

        user_purchases_set.insert((user_id.clone(), product_id.clone()));
        user_orders_map
            .entry(user_id)
            .or_default()
            .push(UserOrderItem {
                order_id,
                product_id,
            });
    }

    println!(
        "Preloaded {} products and {} orders into memory cache.",
        products_map.len(),
        user_purchases_set.len()
    );

    let products = Arc::new(RwLock::new(products_map));
    let categories = Arc::new(RwLock::new(categories_map));
    let user_orders = Arc::new(RwLock::new(user_orders_map));
    let user_purchases = Arc::new(RwLock::new(user_purchases_set));

    // Connect to Redis
    let redis_client = redis::Client::open(redis_url).expect("Invalid Redis URL");
    let redis = redis::aio::ConnectionManager::new(redis_client)
        .await
        .expect("Failed to connect to Redis");

    let state = AppState {
        db,
        redis,
        products,
        categories,
        user_orders,
        user_purchases,
        order_counter: Arc::new(AtomicU64::new(1)),
    };

    // Define routes matching the challenge spec exactly
    let app = Router::new()
        .route("/health", get(health))
        .route("/products", post(create_product).get(list_products))
        .route("/products/{id}", get(get_product))
        .route("/orders", post(create_order))
        .route("/users/{userId}/orders", get(get_user_orders))
        .fallback(not_found_handler)
        .with_state(state);

    let listener = TcpListener::bind(format!("0.0.0.0:{port}"))
        .await
        .expect("Failed to bind listener");

    println!("Server running on 0.0.0.0:{port}");
    axum::serve(listener, app).await.expect("Server failed");
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::Request;
    use tower::ServiceExt;

    fn build_test_app() -> Router {
        Router::<()>::new()
            .route("/health", get(health))
            .fallback(not_found_handler)
    }

    #[tokio::test]
    async fn test_health_endpoint() {
        let app = build_test_app();
        let req = Request::builder()
            .uri("/health")
            .body(axum::body::Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers().get(header::CONTENT_TYPE).unwrap(),
            "application/json"
        );

        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["status"], "ok");
    }

    #[tokio::test]
    async fn test_list_products_sorting() {
        let p1 = Arc::new(ProductItem {
            id: "p_b".into(),
            name: "Product B".into(),
            category: "c1".into(),
            price: 100,
            stock: AtomicI32::new(10),
        });
        let p2 = Arc::new(ProductItem {
            id: "p_a".into(),
            name: "Product A".into(),
            category: "c1".into(),
            price: 100,
            stock: AtomicI32::new(5),
        });
        let p3 = Arc::new(ProductItem {
            id: "p_c".into(),
            name: "Product C".into(),
            category: "c1".into(),
            price: 50,
            stock: AtomicI32::new(2),
        });

        let mut list = vec![p1, p2, p3];
        list.sort_by(|a, b| {
            let p = a.price.cmp(&b.price);
            if p == Ordering::Equal {
                a.id.cmp(&b.id)
            } else {
                p
            }
        });

        assert_eq!(list[0].id, "p_c");
        assert_eq!(list[1].id, "p_a");
        assert_eq!(list[2].id, "p_b");
    }

    #[test]
    fn test_error_response_format() {
        let err = ErrorResponse::new("VALIDATION_ERROR");
        let json = serde_json::to_string(&err).unwrap();
        assert_eq!(json, r#"{"error":"VALIDATION_ERROR"}"#);
    }
}
