use super::*;
use crate::ordering::Ordering;
use prisma_models::prelude::*;
use prisma_query::ast::{row_number, Aliasable, Comparable, Function, Select, Table};

pub struct RelatedNodesWithRowNumber;

impl RelatedNodesQueryBuilder for RelatedNodesWithRowNumber {
    fn with_pagination<'a>(base: RelatedNodesBaseQuery<'a>) -> Select {
        let mut base_query = base.query;

        if let Some(order_by) = base.order_by.as_ref() {
            let column = order_by.field.as_column();

            if !base.selected_fields.columns().contains(&column) {
                base_query = base_query.column(order_by.field.as_column());
            }
        }

        let order_columns = Ordering::aliased_internal(
            Self::BASE_TABLE_ALIAS,
            Self::BASE_TABLE_ALIAS,
            SelectedFields::RELATED_MODEL_ALIAS,
            base.order_by.as_ref(),
            base.is_reverse_order,
        );

        let row_number_part: Function = order_columns
            .into_iter()
            .fold(row_number(), |acc, ord| acc.order_by(ord))
            .partition_by((Self::BASE_TABLE_ALIAS, SelectedFields::PARENT_MODEL_ALIAS))
            .into();

        let with_row_numbers = Select::from_table(Table::from(base_query).alias(Self::BASE_TABLE_ALIAS))
            .value(Table::from(Self::BASE_TABLE_ALIAS).asterisk())
            .value(row_number_part.alias(Self::ROW_NUMBER_ALIAS));

        Select::from_table(Table::from(with_row_numbers).alias(Self::ROW_NUMBER_TABLE_ALIAS))
            .value(Table::from(Self::ROW_NUMBER_TABLE_ALIAS).asterisk())
            .so_that(Self::ROW_NUMBER_ALIAS.between(base.window_limits.0 as i64, base.window_limits.1 as i64))
    }
}
