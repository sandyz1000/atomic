use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;

use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::common::DFSchemaRef;
use datafusion::execution::SessionState;
use datafusion::functions_aggregate::expr_fn::{avg, count, max, min, sum};
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{
    BinaryExpr, Extension, JoinType, LogicalPlan, LogicalPlanBuilder, Operator, SortExpr, col,
    lit,
};

use crate::errors::{NlqError, Result};
use crate::nodes::embed::EmbedNode;
use crate::nodes::llm_filter::LlmFilterNode;
use crate::nodes::llm_map::LlmMapNode;
use crate::nodes::vector_search::VectorSearchNode;

use super::json_plan::{JsonAgg, JsonExpr, JsonPlanNode, JsonSortExpr};

pub struct IrParser {
    session_state: Arc<RwLock<SessionState>>,
    /// Pre-fetched table schemas: name → Arrow SchemaRef
    table_schemas: HashMap<String, SchemaRef>,
    default_model: String,
    default_embed_model: String,
    embed_model_dim: Option<usize>,
    batch_size: usize,
}

impl IrParser {
    pub fn new(
        session_state: Arc<RwLock<SessionState>>,
        table_schemas: HashMap<String, SchemaRef>,
        default_model: String,
        default_embed_model: String,
        embed_model_dim: Option<usize>,
        batch_size: usize,
    ) -> Self {
        Self {
            session_state,
            table_schemas,
            default_model,
            default_embed_model,
            embed_model_dim,
            batch_size,
        }
    }

    pub fn parse(&self, node: &JsonPlanNode) -> Result<LogicalPlan> {
        match node {
            JsonPlanNode::TableScan { table, projection } => {
                self.parse_table_scan(table, projection.as_deref())
            }
            JsonPlanNode::Filter { predicate, input } => {
                let input_plan = self.parse(input)?;
                let schema = input_plan.schema().clone();
                let pred_expr = self.parse_expr(predicate, &schema)?;
                Ok(LogicalPlanBuilder::from(input_plan).filter(pred_expr)?.build()?)
            }
            JsonPlanNode::Projection { exprs, input } => {
                let input_plan = self.parse(input)?;
                let schema = input_plan.schema().clone();
                let proj: Result<Vec<_>> =
                    exprs.iter().map(|e| self.parse_expr(e, &schema)).collect();
                Ok(LogicalPlanBuilder::from(input_plan).project(proj?)?.build()?)
            }
            JsonPlanNode::Aggregate { group_by, aggs, input } => {
                self.parse_aggregate(group_by, aggs, input)
            }
            JsonPlanNode::Sort { exprs, input } => {
                let input_plan = self.parse(input)?;
                let schema = input_plan.schema().clone();
                let sort_exprs: Result<Vec<SortExpr>> =
                    exprs.iter().map(|s| self.parse_sort_expr(s, &schema)).collect();
                Ok(LogicalPlanBuilder::from(input_plan).sort(sort_exprs?)?.build()?)
            }
            JsonPlanNode::Limit { skip, fetch, input } => {
                let input_plan = self.parse(input)?;
                Ok(LogicalPlanBuilder::from(input_plan).limit(*skip, *fetch)?.build()?)
            }
            JsonPlanNode::Join { join_type, left, right, on } => {
                self.parse_join(join_type, left, right, on)
            }
            JsonPlanNode::LlmFilter { prompt, model, input } => {
                let input_plan = self.parse(input)?;
                let model_str = model.clone().unwrap_or_else(|| self.default_model.clone());
                let node = LlmFilterNode::new(prompt.clone(), model_str, input_plan, self.batch_size);
                Ok(LogicalPlan::Extension(Extension { node: Arc::new(node) }))
            }
            JsonPlanNode::LlmMap { prompt, output_col, output_type, model, input } => {
                let input_plan = self.parse(input)?;
                let model_str = model.clone().unwrap_or_else(|| self.default_model.clone());
                let arrow_type = parse_arrow_type(output_type)?;
                let node = LlmMapNode::new(
                    prompt.clone(),
                    model_str,
                    output_col.clone(),
                    arrow_type,
                    input_plan,
                    self.batch_size,
                );
                Ok(LogicalPlan::Extension(Extension { node: Arc::new(node) }))
            }
            JsonPlanNode::Embed { input_col, output_col, model, input } => {
                let input_plan = self.parse(input)?;
                let model_str =
                    model.clone().unwrap_or_else(|| self.default_embed_model.clone());
                let node = EmbedNode::new(
                    input_col.clone(),
                    output_col.clone(),
                    model_str,
                    input_plan,
                    self.embed_model_dim,
                );
                Ok(LogicalPlan::Extension(Extension { node: Arc::new(node) }))
            }
            JsonPlanNode::VectorSearch { query_col, index_name, top_k, input } => {
                let input_plan = self.parse(input)?;
                let node = VectorSearchNode::new(
                    query_col.clone(),
                    index_name.clone(),
                    *top_k,
                    input_plan,
                );
                Ok(LogicalPlan::Extension(Extension { node: Arc::new(node) }))
            }
        }
    }

    fn parse_table_scan(
        &self,
        table: &str,
        projection: Option<&[String]>,
    ) -> Result<LogicalPlan> {
        let schema = self
            .table_schemas
            .get(table)
            .ok_or_else(|| NlqError::Schema(format!("table '{table}' not found in session")))?;

        let proj: Option<Vec<usize>> = if let Some(cols) = projection {
            let indices: Result<Vec<usize>> = cols
                .iter()
                .map(|c| {
                    schema.index_of(c).map_err(|_| {
                        NlqError::Schema(format!("column '{c}' not in table '{table}'"))
                    })
                })
                .collect();
            Some(indices?)
        } else {
            None
        };

        // Use table_scan helper from datafusion-expr which creates a stub plan.
        // DataFusion resolves the real provider from the catalog during execution.
        let builder = datafusion::logical_expr::table_scan(Some(table), schema.as_ref(), proj)
            .map_err(NlqError::DataFusion)?;
        Ok(builder.build().map_err(NlqError::DataFusion)?)
    }

    fn parse_aggregate(
        &self,
        group_by: &[JsonExpr],
        aggs: &[JsonAgg],
        input: &JsonPlanNode,
    ) -> Result<LogicalPlan> {
        let input_plan = self.parse(input)?;
        let schema = input_plan.schema().clone();

        let group_exprs: Result<Vec<_>> =
            group_by.iter().map(|e| self.parse_expr(e, &schema)).collect();

        let agg_exprs: Result<Vec<_>> = aggs
            .iter()
            .map(|agg| {
                let inner = self.parse_expr(&agg.expr, &schema)?;
                let agg_expr = match agg.func.to_lowercase().as_str() {
                    "count" | "count_all" => count(inner),
                    "sum" => sum(inner),
                    "avg" => avg(inner),
                    "min" => min(inner),
                    "max" => max(inner),
                    other => {
                        return Err(NlqError::PlanParse(format!(
                            "unknown aggregation function: {other}"
                        )));
                    }
                };
                Ok(agg_expr.alias(&agg.alias))
            })
            .collect();

        Ok(LogicalPlanBuilder::from(input_plan)
            .aggregate(group_exprs?, agg_exprs?)?
            .build()?)
    }

    fn parse_join(
        &self,
        join_type: &str,
        left: &JsonPlanNode,
        right: &JsonPlanNode,
        on: &[[JsonExpr; 2]],
    ) -> Result<LogicalPlan> {
        let left_plan = self.parse(left)?;
        let right_plan = self.parse(right)?;

        let jt = match join_type.to_lowercase().as_str() {
            "inner" => JoinType::Inner,
            "left" => JoinType::Left,
            "right" => JoinType::Right,
            "full" => JoinType::Full,
            "leftsemi" | "left semi" => JoinType::LeftSemi,
            "leftanti" | "left anti" => JoinType::LeftAnti,
            other => {
                return Err(NlqError::PlanParse(format!("unknown join type: {other}")));
            }
        };

        let left_schema = left_plan.schema().clone();
        let right_schema = right_plan.schema().clone();

        // Build on-clause as a filter expression ANDed together.
        let on_filter: Option<datafusion::logical_expr::Expr> = if on.is_empty() {
            None
        } else {
            let conditions: Result<Vec<_>> = on
                .iter()
                .map(|[l, r]| {
                    let le = self.parse_expr(l, &left_schema)?;
                    let re = self.parse_expr(r, &right_schema)?;
                    Ok(le.eq(re))
                })
                .collect();
            let mut conds = conditions?;
            let first = conds.remove(0);
            let combined = conds.into_iter().fold(first, |acc, c| acc.and(c));
            Some(combined)
        };

        Ok(LogicalPlanBuilder::from(left_plan)
            .join(right_plan, jt, (Vec::<datafusion::common::Column>::new(), Vec::<datafusion::common::Column>::new()), on_filter)?
            .build()?)
    }

    fn parse_sort_expr(&self, s: &JsonSortExpr, schema: &DFSchemaRef) -> Result<SortExpr> {
        let expr = self.parse_expr(&s.expr, schema)?;
        Ok(SortExpr { expr, asc: s.asc, nulls_first: s.nulls_first })
    }

    pub fn parse_expr(
        &self,
        expr: &JsonExpr,
        schema: &DFSchemaRef,
    ) -> Result<datafusion::logical_expr::Expr> {
        use datafusion::logical_expr::Expr;
        match expr {
            JsonExpr::Col { name, table } => {
                // Validate the column exists — gives a clear error instead of DataFusion's cryptic message.
                if schema.field_with_unqualified_name(name).is_err() {
                    return Err(NlqError::Schema(format!(
                        "column '{name}' does not exist in schema (available: {})",
                        schema.fields().iter().map(|f| f.name().as_str()).collect::<Vec<_>>().join(", ")
                    )));
                }
                if let Some(t) = table {
                    Ok(col(format!("{t}.{name}")))
                } else {
                    Ok(col(name.as_str()))
                }
            }
            JsonExpr::Lit { value } => Ok(lit(value.as_str())),
            JsonExpr::LitNum { value } => {
                if let Some(i) = value.as_i64() {
                    Ok(lit(i))
                } else if let Some(f) = value.as_f64() {
                    Ok(lit(f))
                } else {
                    Err(NlqError::PlanParse(format!("cannot parse number: {value}")))
                }
            }
            JsonExpr::LitBool { value } => Ok(lit(*value)),
            JsonExpr::BinOp { op, left, right } => {
                let l = self.parse_expr(left, schema)?;
                let r = self.parse_expr(right, schema)?;
                let operator = parse_operator(op)?;
                Ok(Expr::BinaryExpr(BinaryExpr::new(Box::new(l), operator, Box::new(r))))
            }
            JsonExpr::IsNull { expr } => Ok(self.parse_expr(expr, schema)?.is_null()),
            JsonExpr::IsNotNull { expr } => Ok(self.parse_expr(expr, schema)?.is_not_null()),
            JsonExpr::Not { expr } => {
                Ok(Expr::Not(Box::new(self.parse_expr(expr, schema)?)))
            }
            JsonExpr::Call { func, args } => {
                let state = self.session_state.read();
                let udf = state
                    .scalar_functions()
                    .get(func.as_str())
                    .cloned()
                    .ok_or_else(|| NlqError::PlanParse(format!("unknown function: {func}")))?;
                let arg_exprs: Result<Vec<_>> =
                    args.iter().map(|a| self.parse_expr(a, schema)).collect();
                Ok(Expr::ScalarFunction(ScalarFunction { func: udf, args: arg_exprs? }))
            }
            JsonExpr::Alias { expr, name } => {
                Ok(self.parse_expr(expr, schema)?.alias(name.as_str()))
            }
        }
    }
}

fn parse_operator(op: &str) -> Result<Operator> {
    match op {
        "Eq" | "=" | "==" => Ok(Operator::Eq),
        "NotEq" | "!=" | "<>" => Ok(Operator::NotEq),
        "Lt" | "<" => Ok(Operator::Lt),
        "LtEq" | "<=" => Ok(Operator::LtEq),
        "Gt" | ">" => Ok(Operator::Gt),
        "GtEq" | ">=" => Ok(Operator::GtEq),
        "And" | "and" | "&&" => Ok(Operator::And),
        "Or" | "or" | "||" => Ok(Operator::Or),
        "Plus" | "+" => Ok(Operator::Plus),
        "Minus" | "-" => Ok(Operator::Minus),
        "Multiply" | "*" => Ok(Operator::Multiply),
        "Divide" | "/" => Ok(Operator::Divide),
        other => Err(NlqError::PlanParse(format!("unknown operator: {other}"))),
    }
}

fn parse_arrow_type(type_str: &str) -> Result<DataType> {
    match type_str {
        "Utf8" | "String" => Ok(DataType::Utf8),
        "Int32" => Ok(DataType::Int32),
        "Int64" => Ok(DataType::Int64),
        "Float32" => Ok(DataType::Float32),
        "Float64" => Ok(DataType::Float64),
        "Boolean" | "Bool" => Ok(DataType::Boolean),
        other => Err(NlqError::PlanParse(format!("unknown Arrow type: {other}"))),
    }
}
