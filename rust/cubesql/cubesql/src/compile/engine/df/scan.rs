use std::{any::Any, fmt, sync::Arc};

use async_trait::async_trait;
use cubeclient::models::V1LoadRequestQuery;
use datafusion::{
    arrow::datatypes::{Schema, SchemaRef},
    error::Result,
    execution::context::ExecutionContextState,
    logical_plan::{DFSchemaRef, Expr, LogicalPlan, UserDefinedLogicalNode},
    physical_plan::{
        planner::ExtensionPlanner, DisplayFormatType, ExecutionPlan, Partitioning, PhysicalPlanner,
        SendableRecordBatchStream, Statistics,
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
        assert_eq!(inputs.len(), 0, "input size inconsistent");
        assert_eq!(exprs.len(), 0, "expression size inconsistent");

        Arc::new(CubeScanNode {
            schema: self.schema.clone(),
            request: self.request.clone(),
        })
    }
}

//  Produces an execution plan where the schema is mismatched from
//  the logical plan node.
pub struct CubeScanExtensionPlanner {}

impl ExtensionPlanner for CubeScanExtensionPlanner {
    /// Create a physical plan for an extension node
    fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _ctx_state: &ExecutionContextState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        Ok(
            if let Some(scan_node) = node.as_any().downcast_ref::<CubeScanNode>() {
                assert_eq!(logical_inputs.len(), 0, "Inconsistent number of inputs");
                assert_eq!(physical_inputs.len(), 0, "Inconsistent number of inputs");

                // figure out input name
                Some(Arc::new(CubeScanExecutionPlan {
                    schema: Arc::new(Schema::empty()),
                }))
            } else {
                None
            },
        )
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
