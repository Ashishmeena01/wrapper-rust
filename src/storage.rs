use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePool, SqlitePoolOptions, SqliteSynchronous};
use sqlx::Row;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

#[derive(Debug)]
pub enum StorageError {
    ProductExists,
    ProductNotFound,
    AlreadyPurchased,
    SoldOut,
    Database(sqlx::Error),
}

impl From<sqlx::Error> for StorageError {
    fn from(err: sqlx::Error) -> Self {
        StorageError::Database(err)
    }
}

#[derive(Debug, Clone)]
pub struct DbProduct {
    pub id: String,
    pub name: String,
    pub category: String,
    pub price: i64,
    pub stock: i32,
}

#[derive(Clone)]
pub struct Storage {
    pool: SqlitePool,
    write_lock: Arc<Mutex<()>>,
}

impl Storage {
    pub async fn new(db_path: &str) -> Result<Self, sqlx::Error> {
        // Ensure parent directory exists
        if let Some(parent) = Path::new(db_path).parent() {
            if !parent.as_os_str().is_empty() {
                let _ = tokio::fs::create_dir_all(parent).await;
            }
        }

        let opts = SqliteConnectOptions::new()
            .filename(db_path)
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Normal)
            .busy_timeout(Duration::from_secs(10));

        let pool = SqlitePoolOptions::new()
            .max_connections(50)
            .connect_with(opts)
            .await?;

        // Initialize embedded schema with constraints
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS products (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                category TEXT NOT NULL,
                price INTEGER NOT NULL,
                stock INTEGER NOT NULL CHECK (stock >= 0)
            );",
        )
        .execute(&pool)
        .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_products_cat_price_id ON products(category, price ASC, id ASC);",
        )
        .execute(&pool)
        .await?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS orders (
                order_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                product_id TEXT NOT NULL REFERENCES products(id),
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );",
        )
        .execute(&pool)
        .await?;

        sqlx::query(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_orders_user_product ON orders(user_id, product_id);",
        )
        .execute(&pool)
        .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_orders_user ON orders(user_id);",
        )
        .execute(&pool)
        .await?;

        Ok(Self {
            pool,
            write_lock: Arc::new(Mutex::new(())),
        })
    }

    pub async fn create_product(
        &self,
        id: &str,
        name: &str,
        category: &str,
        price: i64,
        stock: i32,
    ) -> Result<DbProduct, StorageError> {
        let _guard = self.write_lock.lock().await;

        let res = sqlx::query(
            "INSERT INTO products (id, name, category, price, stock) VALUES (?1, ?2, ?3, ?4, ?5)",
        )
        .bind(id)
        .bind(name)
        .bind(category)
        .bind(price)
        .bind(stock)
        .execute(&self.pool)
        .await;

        match res {
            Ok(_) => Ok(DbProduct {
                id: id.to_string(),
                name: name.to_string(),
                category: category.to_string(),
                price,
                stock,
            }),
            Err(sqlx::Error::Database(ref db_err)) if db_err.is_unique_violation() => {
                Err(StorageError::ProductExists)
            }
            Err(e) => Err(StorageError::Database(e)),
        }
    }

    /// Atomically decrement stock and insert order within an embedded SQLite transaction.
    /// Strictly enforces:
    /// 1. Product existence (returns ProductNotFound)
    /// 2. User purchase uniqueness (returns AlreadyPurchased)
    /// 3. Stock >= 0 (returns SoldOut)
    /// All changes are rolled back on any failure.
    pub async fn place_order(
        &self,
        order_id: &str,
        user_id: &str,
        product_id: &str,
    ) -> Result<(), StorageError> {
        let _guard = self.write_lock.lock().await;

        let mut tx = self.pool.begin().await?;

        // 1. Check if product exists and get current stock
        let product_row = sqlx::query("SELECT stock FROM products WHERE id = ?1")
            .bind(product_id)
            .fetch_optional(&mut *tx)
            .await?;

        let current_stock: i32 = match product_row {
            Some(row) => row.get("stock"),
            None => {
                let _ = tx.rollback().await;
                return Err(StorageError::ProductNotFound);
            }
        };

        // 2. Check if user already purchased this product
        let order_row = sqlx::query("SELECT 1 FROM orders WHERE user_id = ?1 AND product_id = ?2")
            .bind(user_id)
            .bind(product_id)
            .fetch_optional(&mut *tx)
            .await?;

        if order_row.is_some() {
            let _ = tx.rollback().await;
            return Err(StorageError::AlreadyPurchased);
        }

        // 3. Check if sold out
        if current_stock <= 0 {
            let _ = tx.rollback().await;
            return Err(StorageError::SoldOut);
        }

        // 4. Atomic decrement with stock > 0 condition
        let update_res = sqlx::query(
            "UPDATE products SET stock = stock - 1 WHERE id = ?1 AND stock > 0",
        )
        .bind(product_id)
        .execute(&mut *tx)
        .await?;

        if update_res.rows_affected() == 0 {
            let _ = tx.rollback().await;
            return Err(StorageError::SoldOut);
        }

        // 5. Insert order record
        let insert_res = sqlx::query(
            "INSERT INTO orders (order_id, user_id, product_id) VALUES (?1, ?2, ?3)",
        )
        .bind(order_id)
        .bind(user_id)
        .bind(product_id)
        .execute(&mut *tx)
        .await;

        match insert_res {
            Ok(_) => {
                tx.commit().await?;
                Ok(())
            }
            Err(sqlx::Error::Database(ref db_err)) if db_err.is_unique_violation() => {
                let _ = tx.rollback().await;
                Err(StorageError::AlreadyPurchased)
            }
            Err(e) => {
                let _ = tx.rollback().await;
                Err(StorageError::Database(e))
            }
        }
    }

    pub async fn get_product(&self, id: &str) -> Result<Option<DbProduct>, sqlx::Error> {
        let row = sqlx::query("SELECT id, name, category, price, stock FROM products WHERE id = ?1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;

        Ok(row.map(|r| DbProduct {
            id: r.get("id"),
            name: r.get("name"),
            category: r.get("category"),
            price: r.get("price"),
            stock: r.get("stock"),
        }))
    }

    pub async fn get_user_orders(&self, user_id: &str) -> Result<Vec<(String, String)>, sqlx::Error> {
        let rows = sqlx::query("SELECT order_id, product_id FROM orders WHERE user_id = ?1")
            .bind(user_id)
            .fetch_all(&self.pool)
            .await?;

        Ok(rows.into_iter().map(|r| (r.get("order_id"), r.get("product_id"))).collect())
    }

    pub async fn load_all_products(&self) -> Result<Vec<DbProduct>, sqlx::Error> {
        let rows = sqlx::query("SELECT id, name, category, price, stock FROM products")
            .fetch_all(&self.pool)
            .await?;

        let products = rows
            .into_iter()
            .map(|r| DbProduct {
                id: r.get("id"),
                name: r.get("name"),
                category: r.get("category"),
                price: r.get("price"),
                stock: r.get("stock"),
            })
            .collect();

        Ok(products)
    }

    pub async fn load_all_orders(&self) -> Result<Vec<(String, String, String)>, sqlx::Error> {
        let rows = sqlx::query("SELECT order_id, user_id, product_id FROM orders")
            .fetch_all(&self.pool)
            .await?;

        let orders = rows
            .into_iter()
            .map(|r| (r.get("order_id"), r.get("user_id"), r.get("product_id")))
            .collect();

        Ok(orders)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_storage_order_atomicity_and_concurrency() {
        // Use in-memory SQLite database for isolated unit test
        let db_id = format!("file:test_{}?mode=memory&cache=shared", std::time::SystemTime::now().elapsed().unwrap_or_default().as_nanos());
        let storage = Storage::new(&db_id).await.expect("Failed to create test storage");

        // 1. Create a product with stock = 1
        let p = storage.create_product("p_single", "Single Item", "cat1", 1000, 1).await.expect("create product failed");
        assert_eq!(p.stock, 1);

        // Duplicate create fails
        let dup = storage.create_product("p_single", "Single Item", "cat1", 1000, 1).await;
        assert!(matches!(dup, Err(StorageError::ProductExists)));

        // 2. First order succeeds
        let res1 = storage.place_order("o_1", "user_1", "p_single").await;
        assert!(res1.is_ok());

        // 3. Duplicate order by same user fails with AlreadyPurchased
        let res_dup = storage.place_order("o_dup", "user_1", "p_single").await;
        assert!(matches!(res_dup, Err(StorageError::AlreadyPurchased)));

        // 4. Second user tries to buy now that stock is 0 -> SoldOut
        let res_sold = storage.place_order("o_2", "user_2", "p_single").await;
        assert!(matches!(res_sold, Err(StorageError::SoldOut)));

        // 5. Verify final stock is exactly 0
        let prod = storage.get_product("p_single").await.unwrap().unwrap();
        assert_eq!(prod.stock, 0);

        // 6. Non-existent product -> ProductNotFound
        let res_missing = storage.place_order("o_3", "user_3", "p_ghost").await;
        assert!(matches!(res_missing, Err(StorageError::ProductNotFound)));
    }
}
