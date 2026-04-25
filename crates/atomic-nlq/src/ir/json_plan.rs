use serde::Deserialize;

/// JSON representation of a query plan produced by the LLM.
///
/// All node variants are present from Phase 1 onward; extension variants
/// (`LlmFilter`, `LlmMap`, `Embed`, `VectorSearch`) return
/// `NlqError::Internal("not yet implemented")` until Phase 2/3.
#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
pub enum JsonPlanNode {
    TableScan {
        table: String,
        projection: Option<Vec<String>>,
    },
    Filter {
        predicate: JsonExpr,
        input: Box<JsonPlanNode>,
    },
    Projection {
        exprs: Vec<JsonExpr>,
        input: Box<JsonPlanNode>,
    },
    Aggregate {
        group_by: Vec<JsonExpr>,
        aggs: Vec<JsonAgg>,
        input: Box<JsonPlanNode>,
    },
    Sort {
        exprs: Vec<JsonSortExpr>,
        input: Box<JsonPlanNode>,
    },
    Limit {
        skip: usize,
        fetch: Option<usize>,
        input: Box<JsonPlanNode>,
    },
    Join {
        join_type: String,
        left: Box<JsonPlanNode>,
        right: Box<JsonPlanNode>,
        on: Vec<[JsonExpr; 2]>,
    },
    LlmFilter {
        prompt: String,
        model: Option<String>,
        input: Box<JsonPlanNode>,
    },
    LlmMap {
        prompt: String,
        output_col: String,
        output_type: String,
        model: Option<String>,
        input: Box<JsonPlanNode>,
    },
    Embed {
        input_col: String,
        output_col: String,
        model: Option<String>,
        input: Box<JsonPlanNode>,
    },
    VectorSearch {
        query_col: String,
        index_name: String,
        top_k: usize,
        input: Box<JsonPlanNode>,
    },
}

/// Expression node in the JSON plan.
#[derive(Debug, Deserialize, Clone)]
#[serde(tag = "type")]
pub enum JsonExpr {
    /// Column reference.
    Col {
        name: String,
        table: Option<String>,
    },
    /// String literal.
    Lit {
        value: String,
    },
    /// Numeric literal.
    LitNum {
        value: serde_json::Number,
    },
    /// Boolean literal.
    LitBool {
        value: bool,
    },
    /// Binary operation.
    BinOp {
        op: String,
        left: Box<JsonExpr>,
        right: Box<JsonExpr>,
    },
    IsNull {
        expr: Box<JsonExpr>,
    },
    IsNotNull {
        expr: Box<JsonExpr>,
    },
    Not {
        expr: Box<JsonExpr>,
    },
    /// UDF call.
    Call {
        func: String,
        args: Vec<JsonExpr>,
    },
    /// Alias.
    Alias {
        expr: Box<JsonExpr>,
        name: String,
    },
}

#[derive(Debug, Deserialize)]
pub struct JsonAgg {
    pub func: String,
    pub expr: JsonExpr,
    pub alias: String,
}

#[derive(Debug, Deserialize)]
pub struct JsonSortExpr {
    pub expr: JsonExpr,
    pub asc: bool,
    pub nulls_first: bool,
}
