use std::sync::Arc;

use async_trait::async_trait;

use crate::transport::TransportService;
use crate::CubeError;

use super::{
    convert_sql_to_cube_query, CompilationResult, MetaContext, QueryPlan,
    QueryPlannerExecutionProps,
};

#[async_trait]
pub trait SqlService: Send + Sync {
    async fn plan(
        &self,
        query: &String,
        ctx: Arc<MetaContext>,
        session: &QueryPlannerExecutionProps,
    ) -> CompilationResult<QueryPlan>;
}

pub struct SqlAuthDefaultImpl {
    transport: Arc<dyn TransportService>,
}

crate::di_service!(SqlAuthDefaultImpl, [SqlService]);

#[async_trait]
impl SqlService for SqlAuthDefaultImpl {
    async fn plan(
        &self,
        query: &String,
        tenant: Arc<MetaContext>,
        session: &QueryPlannerExecutionProps,
    ) -> CompilationResult<QueryPlan> {
        convert_sql_to_cube_query(&query, tenant, self.transport.clone(), session)
    }
}
