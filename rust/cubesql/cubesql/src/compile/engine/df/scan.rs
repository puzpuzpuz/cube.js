use std::{any::Any, fmt, sync::Arc};

use async_trait::async_trait;
use cubeclient::models::V1LoadRequestQuery;
use datafusion::{
    arrow::datatypes::SchemaRef,
    error::Result,
    logical_plan::{DFSchemaRef, Expr, LogicalPlan, UserDefinedLogicalNode},
    physical_plan::{
        DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream, Statistics,
    },
};

#[derive(Debug)]
pub struct CubeScanNode {
    schema: DFSchemaRef,
    request: V1LoadRequestQuery,
}

impl CubeScanNode {
    pub fn new(schema: DFSchemaRef, request: V1LoadRequestQuery) -> Self {
        Self { schema, request }
    }
}

impl UserDefinedLogicalNode for CubeScanNode {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CubeScan: request={:?}", self.request)
    }

    fn from_template(
        &self,
        exprs: &[datafusion::logical_plan::Expr],
        inputs: &[datafusion::logical_plan::LogicalPlan],
    ) -> std::sync::Arc<dyn UserDefinedLogicalNode + Send + Sync> {
        unimplemented!("CubeScan")
    }
}

#[derive(Debug)]
struct CubeScanExecutionPlan {
    schema: SchemaRef,
}

#[async_trait]
impl ExecutionPlan for CubeScanExecutionPlan {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        &self,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        unimplemented!("CubeScanExecutionPlan::with_new_children");
    }

    async fn execute(&self, _partition: usize) -> Result<SendableRecordBatchStream> {
        unimplemented!("CubeScanExecutionPlan::execute");
    }

    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(f, "CubeScanExecutionPlan")
            }
        }
    }

    fn statistics(&self) -> Statistics {
        unimplemented!("CubeScanExecutionPlan::statistics");
    }
}
