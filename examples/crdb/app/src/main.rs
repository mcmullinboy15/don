use std::time::Duration;
use tokio_postgres::NoTls;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("DATABASE_URL")?;
    let (client, connection) = tokio_postgres::connect(&url, NoTls).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {e}");
        }
    });

    // Seed a few rows if the table is empty.
    client
        .execute(
            "INSERT INTO users (email, name) VALUES \
             ('alice@example.com', 'Alice'), \
             ('bob@example.com', 'Bob') \
             ON CONFLICT (email) DO NOTHING",
            &[],
        )
        .await?;

    loop {
        let rows = client
            .query(
                "SELECT email, name, updated_at::TEXT FROM users ORDER BY created_at",
                &[],
            )
            .await?;
        println!("── {} users ──", rows.len());
        for row in &rows {
            let email: &str = row.get(0);
            let name: &str = row.get(1);
            let updated_at: &str = row.get(2);
            println!("   {name} <{email}> (updated at: {updated_at})");
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}
