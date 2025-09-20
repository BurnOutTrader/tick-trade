use crate::models::{Provider, SymbolMeta, UniverseMember};
use crate::duck::{Duck, DuckError};

impl Duck {
    pub fn list_providers(&self) -> Result<Vec<Provider>, DuckError> {
        let mut stmt = self.conn().prepare("SELECT provider, version FROM providers")?;
        let rows = stmt.query_map([], |r| {
            Ok(Provider {
                provider: r.get(0)?,
                version: r.get::<_, Option<String>>(1)?,
            })
        })?;
        Ok(rows.filter_map(Result::ok).collect())
    }

    pub fn list_universe(&self, universe: &str) -> Result<Vec<UniverseMember>, DuckError> {
        let mut stmt = self.conn().prepare(
            "SELECT universe, symbol_id, provider FROM universes WHERE universe = ?",
        )?;
        let rows = stmt.query_map([universe], |r| {
            Ok(UniverseMember { universe: r.get(0)?, symbol_id: r.get(1)?, provider: r.get(2)? })
        })?;
        Ok(rows.filter_map(Result::ok).collect())
    }

    pub fn get_symbol(&self, symbol_id: &str) -> Result<Option<SymbolMeta>, DuckError> {
        let mut stmt = self.conn().prepare(
            "SELECT symbol_id, security, exchange, currency, root, continuous_of FROM symbols WHERE symbol_id=?",
        )?;
        let mut rows = stmt.query([symbol_id])?;
        if let Some(row) = rows.next()? {
            Ok(Some(SymbolMeta {
                symbol_id: row.get(0)?, security: row.get(1)?, exchange: row.get(2)?,
                currency: row.get(3)?, root: row.get(4)?, continuous_of: row.get(5)?,
            }))
        } else { Ok(None) }
    }

    // expose &Connection for internal helpers
    pub(crate) fn conn(&self) -> &duckdb::Connection { &self.conn }
}