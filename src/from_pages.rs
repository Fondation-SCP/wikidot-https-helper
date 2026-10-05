use indicatif::ProgressBar;
use indicatif::ProgressStyle;
use rayon::iter::IntoParallelRefMutIterator;
use rayon::iter::ParallelIterator;
use regex::Regex;
use rusqlite::Connection;
use rusqlite::OpenFlags;
use rusqlite::params;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;
use std::hash::DefaultHasher;
use std::hash::Hasher;
use std::path::PathBuf;
use std::sync::mpsc;
use std::sync::mpsc::Sender;
use std::thread::JoinHandle;
use std::time::Duration;
use std::time::Instant;

#[derive(PartialEq, Eq, Hash, Clone, Debug)]
pub struct PageId {
    pub site: String,
    pub slug: String,
}

struct Page {
    url: String,
    id: PageId,
    source: String,
}

#[derive(PartialEq, Eq, Hash, Serialize, Deserialize, Clone, Debug, PartialOrd, Ord)]
pub struct HostMatch {
    host: String,
    pub requested_path: String,
    pub context: String,
}

#[derive(PartialEq, Eq, Hash, Clone)]
/// A page, viewed as a set of matches over a given host
pub struct PageMatchesOnHost {
    pub page: PageId,
    /// The set of paths requested by the page that this `PageMatchesOnHost` represents on the given host
    pub matches: BTreeSet<HostMatch>,
}

static CONTEXT_MARGIN: usize = 10;

pub fn get_matches(
    site: &String,
    db: &PathBuf,
    cache_db: &PathBuf,
) -> HashMap<String, HashSet<PageMatchesOnHost>> {
    let db = Connection::open_with_flags(db, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .expect("Failed to open a connection to the database");

    let mut pages = {
        let mut stmt = db
            .prepare("SELECT * FROM pages WHERE url LIKE ?")
            .expect("Failed to prepare SQL `SELECT` statement over the source database");
        let pattern = format!("http://{}.wikidot.com/%", site);
        stmt.query_map([pattern], |row| {
            Ok(Page {
                url: row.get("url")?,
                id: PageId {
                    site: site.clone(),
                    slug: row.get("slug")?,
                },
                source: row.get("source")?,
            })
        })
        .expect("Failed to query the database")
        .map(|maybe_page| maybe_page.expect("Failed to iterate through a page"))
        .collect::<Vec<_>>()
    };

    let npages = pages.len() as u64;

    let (cache_db, cache) = prepare_cache(cache_db);
    let (cache_thread, cache_queue) = spawn_cache_thread(cache_db);

    let progress_bar = ProgressBar::new(npages).with_style(
        ProgressStyle::with_template("Looking for hostnames in pages {wide_bar} {pos}/{len}")
            .expect("Failed to create the template for the host search progress bar"),
    );
    let parallel_bar = progress_bar.clone();

    let regex = Regex::new(r#"http://([^/\s"'<>\[\]@|█*,]+)([^\s"'<>\[\]@|█*,]*)"#)
        .expect("Failed to build the regex");

    let parsed_pages = pages
        .par_iter_mut()
        .map(|page| {
            let mut hasher = DefaultHasher::new();
            hasher.write(page.source.as_bytes());
            let hash = hasher.finish() as i64;

            if let Some((cached_hash, matches)) = cache.get(&page.id)
                && *cached_hash == hash
            {
                parallel_bar.println(format!(
                    "{} {}",
                    &page.id.slug,
                    console::style("- cache hit").dim()
                ));
                parallel_bar.inc(1);
                return (page.id.slug.clone(), matches.iter().cloned().collect());
            }

            parallel_bar.println(format!(
                "{} {}",
                &page.id.slug,
                console::style("- searching").dim()
            ));

            let matches: HashSet<HostMatch> = regex
                .captures_iter(&page.source)
                .map(|captures| {
                    let regex_match = captures.get(0).unwrap();
                    let start = page
                        .source
                        .floor_char_boundary(regex_match.start().saturating_sub(CONTEXT_MARGIN));
                    let end = page.source.ceil_char_boundary(
                        (regex_match.end() + CONTEXT_MARGIN).min(page.source.len()),
                    );

                    HostMatch {
                        host: captures[1].to_owned(),
                        requested_path: captures[2]
                            .trim_end_matches(")")
                            .trim_end_matches(");")
                            .to_owned(),
                        context: page.source[start..end].to_owned(),
                    }
                })
                .collect();

            let mut serialized_matches = Vec::new();
            ciborium::into_writer(&matches, &mut serialized_matches)
                .expect("Failed to serialize a match list");

            cache_queue
                .send(Some(Cacheable {
                    url: page.url.clone(),
                    page: page.id.clone(),
                    hash,
                    matches: serialized_matches,
                }))
                .expect("The caching thread is gone");
            parallel_bar.inc(1);
            (page.id.slug.clone(), matches)
        })
        .fold(HashMap::new, |pages_containing, (slug, matches)| {
            group_by_host(
                pages_containing,
                PageMatches {
                    page: PageId {
                        site: site.to_owned(),
                        slug,
                    },
                    matches,
                },
            )
        })
        .reduce(HashMap::new, |mut a, mut b| {
            // always merge the smaller set into the larger one, in order to rehash fewer elements
            if a.len() < b.len() {
                std::mem::swap(&mut a, &mut b);
            }
            for (host, pages) in b {
                a.entry(host).or_default().extend(pages.iter().cloned());
            }
            a
        });

    cache_queue
        .send(None) // signal to the caching thread that we are done
        .expect("The caching thread is gone");
    cache_thread.join().unwrap();
    parsed_pages
}

#[derive(Debug)]
struct PageMatches {
    page: PageId,
    matches: HashSet<HostMatch>,
}

fn group_by_host(
    mut pages_containing: HashMap<String, HashSet<PageMatchesOnHost>>,
    matches: PageMatches,
) -> HashMap<String, HashSet<PageMatchesOnHost>> {
    let hosts = matches.matches.iter().map(|HostMatch { host, .. }| host);
    for host in hosts {
        pages_containing
            .entry(host.clone())
            .or_default()
            .insert(PageMatchesOnHost {
                page: matches.page.clone(),
                matches: matches
                    .matches
                    .iter()
                    .filter_map(|host_match| match host_match {
                        HostMatch {
                            host: matched_host, ..
                        } if host == matched_host => Some(host_match),
                        _ => None,
                    })
                    .cloned()
                    .collect(),
            });
    }
    pages_containing
}

struct Cacheable {
    url: String,
    page: PageId,
    hash: i64, // u64 does not implement rusqlite::types::ToSql
    matches: Vec<u8>,
}

fn spawn_cache_thread(mut db: Connection) -> (JoinHandle<()>, Sender<Option<Cacheable>>) {
    let (tx, rx) = mpsc::channel();
    let cache_thread = std::thread::spawn(move || {
        db.execute("CREATE TABLE IF NOT EXISTS cache (url TEXT PRIMARY KEY, site TEXT, slug TEXT, hash BLOB NOT NULL, matches BLOB NOT NULL)", []).expect("Failed to ensure that the cache table exists");

        let interval = Duration::from_millis(100);
        let mut next_run = Instant::now();

        'thread_loop: loop {
            let start_time = Instant::now();
            std::thread::sleep(next_run.saturating_duration_since(start_time));

            if let Ok(transaction) = db.transaction() {
                for maybe_cacheable in rx.try_iter() {
                    match maybe_cacheable {
                        Some(Cacheable {
                            url,
                            page,
                            hash,
                            matches,
                        }) => {
                            transaction.execute("INSERT INTO cache(url, site, slug, hash, matches) VALUES(?, ?, ?, ?, ?) ON CONFLICT(url) DO UPDATE SET hash=?, matches=?", params![url, page.site, page.slug, hash, matches, hash, matches]).expect("Failed to cache a row");
                        }
                        None => break 'thread_loop,
                    }
                }
                transaction
                    .commit()
                    .expect("Failed to commit a cache transaction");
            }

            next_run = start_time + interval;
        }
    });
    (cache_thread, tx)
}

/// Retrieves matches from whatever is in the cache.
pub fn get_cached_matches(cache_db: &PathBuf) -> HashMap<String, HashSet<PageMatchesOnHost>> {
    let (_, cache) = prepare_cache(cache_db);
    cache
        .into_iter()
        .map(|(page, (_, matches))| PageMatches { page, matches })
        .fold(HashMap::new(), group_by_host)
}

fn prepare_cache(cache_db: &PathBuf) -> (Connection, HashMap<PageId, (i64, HashSet<HostMatch>)>) {
    let cache_db =
        Connection::open(cache_db).expect("Failed to open a connection to the cache database");
    let cache = match cache_db.prepare("SELECT site, slug, hash, matches FROM cache") {
        Ok(mut stmt) => {
            let mut map = HashMap::new();
            stmt.query_map([], |row| {
                Ok((
                    PageId {
                        site: row.get::<_, String>("site")?,
                        slug: row.get::<_, String>("slug")?,
                    },
                    (
                        row.get::<_, i64>("hash")?,
                        row.get::<_, Vec<u8>>("matches")?,
                    ),
                ))
            })
            .expect("Failed to query the cache database")
            .map(|maybe_index| maybe_index.expect("Failed to iterate through a cache row"))
            .for_each(|(url, (hash, serialized_matches))| {
                map.insert(
                    url,
                    (
                        hash,
                        ciborium::from_reader(serialized_matches.as_slice())
                            .expect("Failed to deserialize a match list"),
                    ),
                );
            });
            map
        }
        Err(rusqlite::Error::SqliteFailure(_, Some(msg))) if msg == "no such table: cache" => {
            HashMap::new()
        }
        Err(error) => panic!("Failed to query the cache database: {}", error),
    };
    (cache_db, cache)
}
