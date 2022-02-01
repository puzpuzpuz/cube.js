use std::sync::Arc;

use async_trait::async_trait;

use crate::{transport::TransportService, mysql::AuthContext};
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
        auth_context: Arc<AuthContext>,
        meta_context: Arc<MetaContext>,
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
        auth_context: Arc<AuthContext>,
        meta_context: Arc<MetaContext>,
        session: &QueryPlannerExecutionProps,
    ) -> CompilationResult<QueryPlan> {
        convert_sql_to_cube_query(&query, meta_context, auth_context, self.transport.clone(), session)
    }
}
