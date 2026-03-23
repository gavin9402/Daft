use std::sync::Arc;

use common_error::DaftResult;
use common_treenode::Transformed;

use crate::LogicalPlan;

/// A logical plan optimization rule.
pub trait OptimizerRule {
    /// Returns the name of this optimization rule.
    ///
    /// Defaults to the short type name (last segment of the fully-qualified type path).
    fn name(&self) -> &'static str {
        let full_name = std::any::type_name::<Self>();
        full_name.rsplit("::").next().unwrap_or(full_name)
    }

    /// Try to optimize the logical plan with this rule.
    ///
    /// This returns Transformed::yes(new_plan) if the rule modified the plan, Transformed::no(old_plan) otherwise.
    fn try_optimize(&self, plan: Arc<LogicalPlan>) -> DaftResult<Transformed<Arc<LogicalPlan>>>;
}
