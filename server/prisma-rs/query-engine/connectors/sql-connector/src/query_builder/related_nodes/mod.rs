mod base_query;
mod row_number;

pub use base_query::*;
pub use row_number::*;

use crate::ordering::Ordering;
use prisma_models::prelude::*;
use prisma_query::ast::Select;

pub trait RelatedNodesQueryBuilder {
    const BASE_TABLE_ALIAS: &'static str = "prismaBaseTableAlias";
    const ROW_NUMBER_ALIAS: &'static str = "prismaRowNumberAlias";
    const ROW_NUMBER_TABLE_ALIAS: &'static str = "prismaRowNumberTableAlias";

    fn with_pagination<'a>(base: RelatedNodesBaseQuery<'a>) -> Select;

    fn without_pagination<'a>(base: RelatedNodesBaseQuery<'a>) -> Select {
        let opposite_column = base.from_field.opposite_column().table(Relation::TABLE_ALIAS);
        let order_columns = Ordering::internal(opposite_column, base.order_by.as_ref(), base.is_reverse_order);

        order_columns.into_iter().fold(base.query, |acc, ord| acc.order_by(ord))
    }
}
