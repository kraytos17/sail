// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use datafusion::catalog::Session;
use datafusion::common::{DataFusionError, Result};
use sail_common_datafusion::catalog::LakehouseExecutionContext;
use url::Url;

use crate::options::r#gen::IcebergWriteOptions;
use crate::table::Table;

pub struct PlannerContext<'a> {
    session: &'a dyn Session,
    options: IcebergWriteOptions,
    table_url: Url,
    lakehouse_table: Option<LakehouseExecutionContext>,
    table: Table,
}

impl<'a> PlannerContext<'a> {
    pub async fn new(
        session: &'a dyn Session,
        options: IcebergWriteOptions,
        table_url: Url,
        lakehouse_table: Option<LakehouseExecutionContext>,
    ) -> Result<Self> {
        let table = Table::load(session, table_url.clone()).await?;
        Ok(Self {
            session,
            options,
            table_url,
            lakehouse_table,
            table,
        })
    }

    pub fn session(&self) -> &dyn Session {
        self.session
    }

    pub fn table_url(&self) -> &Url {
        &self.table_url
    }

    pub fn options(&self) -> &IcebergWriteOptions {
        &self.options
    }

    pub fn lakehouse_table(&self) -> Option<&LakehouseExecutionContext> {
        self.lakehouse_table.as_ref()
    }

    pub fn table(&self) -> &Table {
        &self.table
    }

    /// Resolve the object store for the target table. Reserved API surface for future
    /// planner needs; no current planner helper consumes it.
    pub fn object_store(&self) -> Result<Arc<dyn object_store::ObjectStore>> {
        self.session
            .runtime_env()
            .object_store_registry
            .get_store(&self.table_url)
            .map_err(|e| DataFusionError::External(Box::new(e)))
    }
}
