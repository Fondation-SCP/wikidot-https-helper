use std::path::PathBuf;

use clap::Parser;
use rusqlite::Connection;

#[derive(Parser)]
struct Cli {
    /// The path to the SQLite database to use.
    db: PathBuf,
    /// The Wikidot site handle (e.g. `scp-wiki`)
    site: String,
}

fn main() {
    let args = Cli::parse();

    let db = Connection::open(args.db).expect("Failed to open a connection to the database");

    let mut stmt = db
        .prepare("SELECT * FROM pages WHERE url LIKE ?")
        .expect("Failed to prepare SQL `SELECT` statement");
    let pattern = format!("http://{}.wikidot.com/%", args.site);
    let page_iter = stmt
        .query_map([pattern], |row| Ok(row.get::<_, String>("slug")?))
        .expect("Failed to query the database");

    dbg!(page_iter.collect::<Vec<_>>().len());
}
