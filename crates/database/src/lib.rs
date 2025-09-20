pub mod models;
pub mod layout;
pub mod parquet;
pub mod duck;
pub mod catalog;
pub mod queries;
pub mod paths;
pub mod append;
pub mod ingest;
pub mod duck_queries;
mod merge;
mod replay;
// ⬇️ declare the file so the module exists


// then re-export the functions
pub use queries::{
    get_ticks_in_range, get_ticks_from_date_to_latest,
    get_candles_in_range, get_candles_from_date_to_latest,
    get_books_in_range, get_latest_book,
    earliest_book_available, latest_book_available,
};
