use indicatif::ParallelProgressIterator;
use rayon::iter::IntoParallelRefMutIterator;
use rayon::iter::ParallelIterator;
use std::{borrow::Cow, path::PathBuf};

use clap::Parser;
use ftml::{
    data::{PageInfo, ScoreValue},
    layout::Layout,
    prelude::{WikitextMode, WikitextSettings},
};
use rusqlite::Connection;

#[derive(Parser)]
struct Cli {
    /// The path to the SQLite database to use.
    db: PathBuf,
    /// The Wikidot site handle (e.g. `scp-wiki`)
    site: String,
}

struct Page {
    slug: String,
    source: String,
    title: String,
    category: String,
}

fn main() {
    let args = Cli::parse();

    let db = Connection::open(args.db).expect("Failed to open a connection to the database");

    let mut stmt = db
        .prepare("SELECT * FROM pages WHERE url LIKE ?")
        .expect("Failed to prepare SQL `SELECT` statement");
    let pattern = format!("http://{}.wikidot.com/%", args.site);
    let mut pages: Vec<_> = stmt
        .query_map([pattern], |row| {
            Ok(Page {
                slug: row.get("slug")?,
                source: row.get("source")?,
                title: row.get("title")?,
                category: row.get("category")?,
            })
        })
        .expect("Failed to query the database")
        .map(|maybe_page| maybe_page.expect("Failed to iterate through a page"))
        .collect();

    let parse_settings = WikitextSettings::from_mode(WikitextMode::Page, Layout::Wikidot);

    let npages = pages.len() as u64;

    let nwarnings: usize = pages
        .par_iter_mut()
        .progress_count(npages)
        .map(|page| {
            let page_info = PageInfo {
                page: Cow::Borrowed(&page.slug),
                category: match &*page.category {
                    "_default" => None, // according to docs: https://docs.rs/ftml/1.41.0/ftml/data/struct.PageInfo.html#structfield.category
                    category => Some(Cow::Borrowed(category)),
                },
                site: Cow::Borrowed(&args.site),
                title: Cow::Borrowed(&page.title),
                alt_title: None, // not worth the hassle to try to fetch it
                score: ScoreValue::Integer(0), // same
                tags: Vec::new(), // same
                language: Cow::Borrowed("en"), // same
            };

            ftml::preprocess(&mut page.source);
            let tokens = ftml::tokenize(&page.source);
            let (tree, warnings) = ftml::parse(&tokens, &page_info, &parse_settings).into();
            warnings.len()
        })
        .sum();

    println!("{} warnings generated", nwarnings);
}
