use rustler::{Encoder, Env, Resource, ResourceArc, Term};

use crate::database::ExDuckDBRef;

/// A reference-counted temporary table in DuckDB.
/// When the last Elixir reference is garbage collected,
/// Rust's Drop impl automatically executes DROP TABLE.
pub struct ExTempTableRef {
    pub table_name: String,
    pub db: ResourceArc<ExDuckDBRef>,
}

#[rustler::resource_impl]
impl Resource for ExTempTableRef {}

impl Drop for ExTempTableRef {
    fn drop(&mut self) {
        // Auto-cleanup: drop the temp table when all references are gone
        if let Ok(conn) = self.db.conn.lock() {
            let _ = conn.execute_batch(&format!(
                "DROP TABLE IF EXISTS {}",
                self.table_name
            ));
        }
    }
}

pub struct ExTempTable {
    pub resource: ResourceArc<ExTempTableRef>,
}

impl ExTempTable {
    pub fn new(table_name: String, db: ResourceArc<ExDuckDBRef>) -> Self {
        Self {
            resource: ResourceArc::new(ExTempTableRef { table_name, db }),
        }
    }

}

impl Encoder for ExTempTable {
    fn encode<'a>(&self, env: Env<'a>) -> Term<'a> {
        // Return a tuple {table_name, resource_ref} so Elixir can access both
        (self.resource.table_name.as_str(), self.resource.clone()).encode(env)
    }
}

impl<'a> rustler::Decoder<'a> for ExTempTable {
    fn decode(term: Term<'a>) -> rustler::NifResult<Self> {
        // Decode from just the resource reference
        let resource: ResourceArc<ExTempTableRef> = term.decode()?;
        Ok(ExTempTable { resource })
    }
}
