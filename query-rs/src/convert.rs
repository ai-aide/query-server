use crate::CustomError;
use anyhow::{Result, anyhow};
use polars::prelude::*;
use polars_plan::plans::{DynLiteralValue, LiteralValue};
use sqlparser::ast::{
    BinaryOperator as SqlBinaryOperator, Expr as SqlExpr, Ident, LimitClause, ObjectNamePart,
    Offset as SqlOffset, OrderBy, OrderByKind, Select, SelectItem, SetExpr, Statement, TableFactor,
    TableWithJoins, Value as SqlValue, ValueWithSpan,
};

/// Custom Sql struct
pub struct Sql<'a> {
    pub(crate) selection: Vec<Expr>,
    pub(crate) condition: Option<Expr>,
    pub(crate) source: &'a str,
    pub(crate) order_by: Vec<(String, OrderType)>,
    pub(crate) offset: Option<i64>,
    pub(crate) limit: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum OrderType {
    Asc,
    Desc,
}

pub struct InterimExpr(pub(crate) Box<SqlExpr>);
pub struct InterimOperator(pub(crate) SqlBinaryOperator);
// Selection item, example: age > 10
pub struct InterimSelectItem<'a>(pub(crate) &'a SelectItem);
// Source table
pub struct InterimSource<'a>(pub(crate) &'a [TableWithJoins]);
// Order formula, example: order by member_id
pub struct InterimOrderBy<'a>(pub(crate) &'a OrderBy);

pub struct InterimOffset<'a>(pub(crate) &'a SqlOffset);
pub struct InterimLimit<'a>(pub(crate) &'a SqlExpr);
pub struct InterimValue(pub(crate) SqlValue);

/// Convert sqlparser statement to Custom Sql struct
impl<'a> TryFrom<&'a Statement> for Sql<'a> {
    type Error = CustomError;

    fn try_from(sql: &'a Statement) -> Result<Self, Self::Error> {
        match sql {
            Statement::Query(q) => {
                // limit and offset
                let (limit, offset) = match &q.limit_clause {
                    Some(LimitClause::LimitOffset { limit, offset, .. }) => {
                        (limit.as_ref(), offset.as_ref())
                    }
                    _ => (None, None),
                };
                let limit = limit.map(|v| InterimLimit(v).into());
                let offset = offset.map(|v| InterimOffset(v).into());

                // order by
                let mut order_by = Vec::new();
                let orders = q.order_by.as_ref();
                if let Some(expr) = orders {
                    order_by = InterimOrderBy(expr).try_into()?;
                }

                // Select, including table, selection, projection
                let Select {
                    from: table_with_joins,
                    selection: where_clause,
                    projection,

                    group_by: _,
                    ..
                } = match q.body.as_ref() {
                    SetExpr::Select(statement) => statement.as_ref(),
                    v => return Err(CustomError::SqlExpressionError(v.to_string())),
                };
                let source = InterimSource(table_with_joins).try_into()?;

                let condition = match where_clause {
                    Some(expr) => Some(InterimExpr(Box::new(expr.to_owned())).try_into()?),
                    None => None,
                };

                let mut selection = Vec::with_capacity(8);
                for p in projection {
                    let expr = InterimSelectItem(p).try_into()?;
                    selection.push(expr);
                }

                Ok(Sql {
                    selection,
                    source,
                    limit,
                    offset,
                    condition,
                    order_by,
                })
            }
            v => Err(CustomError::SqlStatementError(format!("{:?}", v))),
        }
    }
}

/// Convert SqlParser Expr To DataFrame Expr
impl TryFrom<InterimExpr> for Expr {
    type Error = CustomError;

    fn try_from(expr: InterimExpr) -> std::result::Result<Self, Self::Error> {
        match *expr.0 {
            SqlExpr::BinaryOp { left, op, right } => Ok(Expr::BinaryExpr {
                left: Arc::new(InterimExpr(left).try_into()?),
                op: InterimOperator(op).try_into()?,
                right: Arc::new(InterimExpr(right).try_into()?),
            }),
            SqlExpr::Wildcard(_num) => Ok(Self::Wildcard),
            SqlExpr::Identifier(ident) => {
                for op in ["=", ">", ">=", "<", "<="].into_iter() {
                    if let Some((left, right)) = ident.value.split_once(op) {
                        let temp_op: InterimOperator = op.try_into()?;
                        return Ok(Expr::BinaryExpr {
                            left: Arc::new(
                                InterimExpr(Box::new(SqlExpr::Identifier(Ident::new(left))))
                                    .try_into()?,
                            ),
                            op: temp_op.try_into()?,
                            right: Arc::new(
                                InterimExpr(Box::new(SqlExpr::Value(
                                    SqlValue::Number(right.to_owned(), true).into(),
                                )))
                                .try_into()?,
                            ),
                        });
                    }
                }
                Ok(Self::Column(ident.value.into()))
            }
            SqlExpr::Value(v) => Ok(Self::Literal(InterimValue(v.value).try_into()?)),
            v => Err(CustomError::SqlExpressionError(format!("{}", v))),
        }
    }
}

/// Convert SqlParser BinaryOperator To DataFrame Operator
impl TryFrom<InterimOperator> for Operator {
    type Error = CustomError;

    fn try_from(op: InterimOperator) -> std::result::Result<Self, Self::Error> {
        match op.0 {
            SqlBinaryOperator::Plus => Ok(Self::Plus),
            SqlBinaryOperator::Minus => Ok(Self::Minus),
            SqlBinaryOperator::Multiply => Ok(Self::Multiply),
            SqlBinaryOperator::Divide => Ok(Self::Divide),
            SqlBinaryOperator::Modulo => Ok(Self::Modulus),
            SqlBinaryOperator::Gt => Ok(Self::Gt),
            SqlBinaryOperator::Lt => Ok(Self::Lt),
            SqlBinaryOperator::GtEq => Ok(Self::GtEq),
            SqlBinaryOperator::LtEq => Ok(Self::LtEq),
            SqlBinaryOperator::Eq => Ok(Self::Eq),
            SqlBinaryOperator::NotEq => Ok(Self::NotEq),
            SqlBinaryOperator::And => Ok(Self::And),
            SqlBinaryOperator::Or => Ok(Self::Or),
            v => Err(CustomError::SqlOperatorError(v.to_string())),
        }
    }
}

/// Convert &str(>,>=,<,<=,=) to Interim Operation
impl TryFrom<&str> for InterimOperator {
    type Error = CustomError;

    fn try_from(value: &str) -> std::result::Result<Self, Self::Error> {
        match value {
            ">" => Ok(InterimOperator(SqlBinaryOperator::Gt)),
            ">=" => Ok(InterimOperator(SqlBinaryOperator::GtEq)),
            "=" => Ok(InterimOperator(SqlBinaryOperator::Eq)),
            "<" => Ok(InterimOperator(SqlBinaryOperator::Lt)),
            "<=" => Ok(InterimOperator(SqlBinaryOperator::LtEq)),
            _ => Err(CustomError::SqlOperatorError(value.to_owned())),
        }
    }
}

/// Convert SqlParser SelectItem to Expr of polars
impl<'a> TryFrom<InterimSelectItem<'a>> for Expr {
    type Error = CustomError;

    fn try_from(p: InterimSelectItem<'a>) -> std::result::Result<Self, Self::Error> {
        match p.0 {
            SelectItem::UnnamedExpr(SqlExpr::Identifier(id)) => Ok(col(&id.to_string())),
            SelectItem::ExprWithAlias {
                expr: SqlExpr::Identifier(id),
                alias,
            } => Ok(Expr::Alias(
                Arc::new(Expr::Column((&id.value).into())),
                (&alias.value).to_owned().into(),
            )),
            item => Err(CustomError::SqlSelectItemError(item.to_string())),
        }
    }
}

impl<'a> TryFrom<InterimSource<'a>> for &'a str {
    type Error = CustomError;

    fn try_from(source: InterimSource<'a>) -> Result<Self, Self::Error> {
        // ToDo
        if source.0.len() != 1 {
            return Err(CustomError::SqlTableError("empty".to_string()));
        }

        let table = &source.0[0];
        if !table.joins.is_empty() {
            return Err(CustomError::SqlTableError(format!(
                "joint table {:?}",
                table.joins
            )));
        }

        match &table.relation {
            TableFactor::Table { name, .. } => {
                let Some(ObjectNamePart::Identifier(ident)) = &name.0.first() else {
                    return Err(CustomError::SqlTableError(format!("{:?}", &name.0)));
                };
                Ok(&ident.value)
            }
            v => Err(CustomError::SqlTableError(format!("{:?}", v))),
        }
    }
}

/// Convert SqlParser order by expr to Vec<(String, OrderType)>
impl<'a> TryFrom<InterimOrderBy<'a>> for Vec<(String, OrderType)> {
    type Error = CustomError;

    fn try_from(o: InterimOrderBy<'a>) -> Result<Self, Self::Error> {
        let order_list = match &o.0.kind {
            OrderByKind::Expressions(order_by_list) => {
                let order_list = order_by_list.iter().rfold(
                    Vec::new(),
                    |mut acc: Vec<(String, OrderType)>, order_by| {
                        if let SqlExpr::Identifier(id) = &order_by.expr {
                            let order_type = if let Some(is_asc) = order_by.options.asc {
                                if is_asc == true {
                                    OrderType::Asc
                                } else {
                                    OrderType::Desc
                                }
                            } else if let Some((_, order_type)) = acc.last() {
                                order_type.to_owned()
                            } else {
                                OrderType::Desc
                            };
                            acc.push((id.value.to_string(), order_type));
                        } else {
                            // return Err(CustomError::SqlOrderError(order_by.expr);
                            println!(
                                "We only support identifier for order by, get {}",
                                &order_by.expr
                            );
                        }
                        acc
                    },
                );

                order_list.iter().rev().cloned().collect()
            }
            _ => vec![],
        };

        Ok(order_list)
    }
}

/// Convert SqlParser offset expr to i64
impl<'a> From<InterimOffset<'a>> for i64 {
    fn from(offset: InterimOffset<'a>) -> Self {
        match offset.0 {
            SqlOffset {
                value:
                    SqlExpr::Value(ValueWithSpan {
                        value: SqlValue::Number(v, _b),
                        ..
                    }),
                ..
            } => v.parse().unwrap_or(0),
            _ => 0,
        }
    }
}

/// Convert SqlParser limit expr to usize
impl<'a> From<InterimLimit<'a>> for usize {
    fn from(l: InterimLimit<'a>) -> Self {
        match l.0 {
            SqlExpr::Value(ValueWithSpan { value, .. }) => {
                if let SqlValue::Number(v, _b) = value {
                    v.parse().unwrap_or(usize::MAX)
                } else {
                    100
                }
            }
            _ => usize::MAX,
        }
    }
}

/// Convert SqlParser Value to LiteralValue of polars
impl TryFrom<InterimValue> for LiteralValue {
    type Error = CustomError;

    fn try_from(value: InterimValue) -> Result<Self, Self::Error> {
        match value.0 {
            SqlValue::Number(v, _) => Ok(LiteralValue::Dyn(DynLiteralValue::Float(
                v.parse().unwrap_or_default(),
            ))),
            v => Err(CustomError::SqlValueError(format!("{}", v))),
            // v => Err(anyhow!("Value {} is not supported", v)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TyrDialect;
    use sqlparser::parser::Parser;

    #[test]
    fn parse_sql_works() {
        let url = "http://abc.xyz/abc?a=1&b=2";
        let sql = format!(
            "SELECT
                a, 
                b, 
                c 
            FROM {} 
            WHERE a = 100 and b = 200 and c=300 
            ORDER BY c, e DESC, b ASC 
            LIMIT 5 OFFSET 10",
            url
        );
        let statement = &Parser::parse_sql(&TyrDialect::default(), sql.as_ref()).unwrap()[0];
        let sql: Sql = statement.try_into().unwrap();
        // verify data source
        assert_eq!(sql.source, url);
        let fisrt_condition = Expr::BinaryExpr {
            left: Arc::new(Expr::Column("a".into())),
            op: Operator::Eq,
            right: Arc::new(Expr::Literal(LiteralValue::Dyn(DynLiteralValue::Float(
                100 as f64,
            )))),
        };
        let second_condition = Expr::BinaryExpr {
            left: Arc::new(Expr::Column("b".into())),
            op: Operator::Eq,
            right: Arc::new(Expr::Literal(LiteralValue::Dyn(DynLiteralValue::Float(
                200 as f64,
            )))),
        };
        let third_condition = Expr::BinaryExpr {
            left: Arc::new(Expr::Column("c".into())),
            op: Operator::Eq,
            right: Arc::new(Expr::Literal(LiteralValue::Dyn(DynLiteralValue::Float(
                300 as f64,
            )))),
        };
        let inner_conditon = Expr::BinaryExpr {
            left: Arc::new(fisrt_condition),
            op: Operator::And,
            right: Arc::new(second_condition),
        };
        let condition = Expr::BinaryExpr {
            left: Arc::new(inner_conditon),
            op: Operator::And,
            right: Arc::new(third_condition),
        };
        // verify select condition
        assert_eq!(sql.condition, Some(condition));
        assert_eq!(sql.limit, Some(5));
        assert_eq!(sql.offset, Some(10));
        // verify order by
        assert_eq!(
            sql.order_by,
            vec![
                ("c".into(), OrderType::Desc),
                ("e".into(), OrderType::Desc),
                ("b".into(), OrderType::Asc)
            ]
        );
        // verify select item
        assert_eq!(sql.selection, vec![col("a"), col("b"), col("c")]);
    }
}
