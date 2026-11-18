use axum::{extract::State, http::StatusCode, routing::get, Json, Router};
use redis::AsyncCommands;
use serde::Serialize;
use sqlx::{postgres::PgPoolOptions, PgPool};
use std::{env, time::Duration};
use tokio::net::TcpListener;


#[derive(Clone)]
struct AppState {
    db: PgPool,
    redis: redis::aio::ConnectionManager,
    cache_ttl: u64,
}


#[derive(Debug, Serialize, sqlx::FromRow)]
struct Product {
    id: i32,
    name: String,
    price_cents: i32,
}


async fn health() -> &'static str {
    "ok\n"
}


async fn products(State(state): State<AppState>) -> Result<Json<Vec<Product>>, (StatusCode, String)> {
    const CACHE_KEY: &str = "products:v1";

    // Cache read: if Redis has the JSON payload, return it immediately.
    let mut redis = state.redis.clone();
    let cached: Option<String> = redis
        .get(CACHE_KEY)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("Redis GET: {e}")))?;

    if let Some(json) = cached {
        let items: Vec<Product> = serde_json::from_str(&json)
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("Cache JSON: {e}")))?;
        return Ok(Json(items));
    }

    // Cache miss: query PostgreSQL, serialize, and cache for the configured TTL.
    let items = sqlx::query_as::<_, Product>(
        "SELECT id, name, price_cents FROM products ORDER BY id"
    )
    .fetch_all(&state.db)
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("Postgres query: {e}")))?;

    let json = serde_json::to_string(&items)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("Serialize: {e}")))?;
    let _: () = redis
        .set_ex(CACHE_KEY, json, state.cache_ttl)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("Redis SET: {e}")))?;

    Ok(Json(items))
}


#[tokio::main]
async fn main() {
    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let redis_url = env::var("REDIS_URL").unwrap_or_else(|_| "redis://redis:6379".into());
    let port: u16 = env::var("PORT").unwrap_or_else(|_| "8080".into()).parse().expect("PORT must be a number");
    let cache_ttl: u64 = env::var("CACHE_TTL_SECONDS").unwrap_or_else(|_| "30".into()).parse().expect("CACHE_TTL_SECONDS must be a number");

    let db = PgPoolOptions::new()
        .max_connections(20)
        .min_connections(1)
        .acquire_timeout(Duration::from_secs(5))
        .connect(&database_url)
        .await
        .expect("Failed to connect to PostgreSQL");

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS products (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            price_cents INTEGER NOT NULL
        )"
    )
    .execute(&db)
    .await
    .expect("Failed to create products table");

    sqlx::query(
        "INSERT INTO products (name, price_cents) VALUES
            ('Keyboard', 4999), ('Mouse', 1999), ('Monitor', 15999)
         ON CONFLICT DO NOTHING"
    )
    .execute(&db)
    .await
    .expect("Failed to seed sample products");

    let redis_client = redis::Client::open(redis_url).expect("Invalid Redis URL");
    let redis = redis::aio::ConnectionManager::new(redis_client)
        .await
        .expect("Failed to connect to Redis");

    let state = AppState { db, redis, cache_ttl };
    let app = Router::new()
        .route("/", get(health))
        .route("/health", get(health))
        .route("/products", get(products))
        .with_state(state);

    let listener = TcpListener::bind(format!("0.0.0.0:{port}"))
        .await
        .expect("Failed to bind listener");
    println!("API listening on 0.0.0.0:{port}");
    axum::serve(listener, app).await.expect("Server failed");
}
