use anyhow::{Result, anyhow};
use polars::prelude::*;
// use polars
use polars_plan::plans::{DynLiteralValue, LiteralValue};
use sqlparser::ast::{
    BinaryOperator as SqlBinaryOperator, Expr as SqlExpr, LimitClause, ObjectNamePart,
    Offset as SqlOffset, OrderBy, OrderByKind, OrderByOptions, Select, SelectItem, SetExpr,
    Statement, TableFactor, TableWithJoins, Value as SqlValue, ValueWithSpan,
};

/// 解析出来的 Sql
pub struct Sql<'a> {
    pub(crate) selection: Vec<Expr>,
    pub(crate) condition: Option<Expr>,
    pub(crate) source: &'a str,
    pub(crate) order_by: Vec<(String, bool)>,
    pub(crate) offset: Option<i64>,
    pub(crate) limit: Option<usize>,
}

// 因为 Rust trait 的孤儿原则，对已有的类型实现已有的 trait
// 所以这里不能直接用
pub struct Expression(pub(crate) Box<SqlExpr>);
pub struct Operation(pub(crate) SqlBinaryOperator);
pub struct Projection<'a>(pub(crate) &'a SelectItem);
pub struct Source<'a>(pub(crate) &'a [TableWithJoins]);
pub struct Order<'a>(pub(crate) &'a OrderBy);
pub struct Offset<'a>(pub(crate) &'a SqlOffset);
pub struct Limit<'a>(pub(crate) &'a SqlExpr);
pub struct Value(pub(crate) SqlValue);

/// 把 SqlParser 解析出来的 Statement 转换成我们需要的结构
impl<'a> TryFrom<&'a Statement> for Sql<'a> {
    type Error = anyhow::Error;

    fn try_from(sql: &'a Statement) -> Result<Self, Self::Error> {
        match sql {
            // 目前只关心 query (select ... from ... where ...)
            Statement::Query(q) => {
                let (limit, offset) = match &q.limit_clause {
                    Some(LimitClause::LimitOffset { limit, offset, .. }) => {
                        (limit.as_ref(), offset.as_ref())
                    }
                    _ => (None, None),
                };
                let orders = q.order_by.as_ref();
                let Select {
                    from: table_with_joins,
                    selection: where_clause,
                    projection,

                    group_by: _,
                    ..
                } = match q.body.as_ref() {
                    SetExpr::Select(statement) => statement.as_ref(),
                    _ => return Err(anyhow!("We only support Select Query at the moment")),
                };

                let source = Source(table_with_joins).try_into()?;

                println!("---- where expression: {:#?}\n\n", where_clause);
                let condition = match where_clause {
                    Some(expr) => Some(Expression(Box::new(expr.to_owned())).try_into()?),
                    None => None,
                };

                let mut selection = Vec::with_capacity(8);
                for p in projection {
                    let expr = Projection(p).try_into()?;
                    selection.push(expr);
                }

                let mut order_by = Vec::new();
                if let Some(expr) = orders {
                    let order = Order(expr).try_into()?;
                    order_by.push(order);
                }

                let limit = limit.map(|v| Limit(v).into());
                let offset = offset.map(|v| Offset(v).into());

                Ok(Sql {
                    selection,
                    source,
                    limit,
                    offset,
                    condition,
                    order_by,
                })
                // Err(anyhow!("We only support Select Query at the moment"))
            }
            _ => Err(anyhow!("We only support Query at the moment")),
        }
    }
}

/// 把 SqlParser 的 Expr 转换成 DataFrame 的 Expr
impl TryFrom<Expression> for Expr {
    type Error = anyhow::Error;

    fn try_from(expr: Expression) -> std::result::Result<Self, Self::Error> {
        match *expr.0 {
            SqlExpr::BinaryOp { left, op, right } => Ok(Expr::BinaryExpr {
                left: Arc::new(Expression(left).try_into()?),
                op: Operation(op).try_into()?,
                right: Arc::new(Expression(right).try_into()?),
            }),
            SqlExpr::Wildcard(_num) => Ok(Self::Wildcard),
            SqlExpr::Identifier(ident) => Ok(Self::Column(ident.value.into())),
            SqlExpr::Value(v) => {
                // Err(anyhow!("expr value {} is not supported", value))
                // Self::var(self, ddof)
                Ok(Self::Literal(Value(v.value).try_into()?))
            }
            v => Err(anyhow!("expr {:#?} is not supported", v)),
        }
    }
}

/// 把 SqlParser 的 BinaryOperator 转换成 DataFrame 的 Operator
impl TryFrom<Operation> for Operator {
    type Error = anyhow::Error;

    fn try_from(op: Operation) -> std::result::Result<Self, Self::Error> {
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
            v => Err(anyhow!("Operator {} is not supported", v)),
        }
    }
}

/// 把 SqlParser 的 SelectItem 转换成 DataFrame 的 Expr
impl<'a> TryFrom<Projection<'a>> for Expr {
    type Error = anyhow::Error;

    fn try_from(p: Projection<'a>) -> std::result::Result<Self, Self::Error> {
        match p.0 {
            SelectItem::UnnamedExpr(SqlExpr::Identifier(id)) => Ok(col(&id.to_string())),
            SelectItem::ExprWithAlias {
                expr: SqlExpr::Identifier(id),
                alias,
            } => Ok(Expr::Alias(
                Arc::new(Expr::Column((&id.value).into())),
                (&alias.value).to_owned().into(),
            )),
            item => Err(anyhow!("projection {} not supported", item)),
        }
    }
}

impl<'a> TryFrom<Source<'a>> for &'a str {
    type Error = anyhow::Error;

    fn try_from(source: Source<'a>) -> Result<Self, Self::Error> {
        if source.0.len() != 1 {
            return Err(anyhow!("We only support single data source at the moment"));
        }

        let table = &source.0[0];
        if !table.joins.is_empty() {
            return Err(anyhow!("We do not support joint data source at the moment"));
        }

        match &table.relation {
            TableFactor::Table { name, .. } => {
                let Some(ObjectNamePart::Identifier(ident)) = &name.0.first() else {
                    return Err(anyhow!("We only support table"));
                };
                Ok(&ident.value)
            }
            _ => Err(anyhow!("We only support table")),
        }
    }
}

/// 把 SqlParser 的 order by expr 转换成（列名，排序方法）
impl<'a> TryFrom<Order<'a>> for (String, bool) {
    type Error = anyhow::Error;

    fn try_from(o: Order<'a>) -> Result<Self, Self::Error> {
        let (name, is_asc) = match &o.0.kind {
            OrderByKind::Expressions(order_by_list) => {
                let order_by = order_by_list
                    .first()
                    .ok_or_else(|| anyhow!("Unsupported order by kind"))?;

                let name = match &order_by.expr {
                    SqlExpr::Identifier(id) => &id.value,
                    expr => {
                        return Err(anyhow!(
                            "We only support identifier for order by, got {}",
                            expr
                        ));
                    }
                };
                let is_asc = order_by.options.asc.unwrap_or(false);
                (name.to_string(), is_asc)
            }
            _ => (String::new(), false),
        };

        Ok((name, is_asc))
    }
}

/// 把 SqlParser 的 offset expr 转换成 i64
impl<'a> From<Offset<'a>> for i64 {
    fn from(offset: Offset<'a>) -> Self {
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

/// 把 SqlParser 的 Limit expr 转换成 usize
impl<'a> From<Limit<'a>> for usize {
    fn from(l: Limit<'a>) -> Self {
        match l.0 {
            SqlExpr::Value(ValueWithSpan { value, .. }) => {
                if let SqlValue::Number(v, _b) = value {
                    v.parse().unwrap_or(usize::MAX)
                } else {
                    usize::MAX
                }
            }
            _ => usize::MAX,
        }
    }
}

/// 把 SqlParser 的 Value 转换成 DataFrame 支持的 LiteralValue
impl TryFrom<Value> for LiteralValue {
    type Error = anyhow::Error;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value.0 {
            SqlValue::Number(v, _a) => Ok(LiteralValue::Dyn(DynLiteralValue::Float(
                v.parse().unwrap_or_default(),
            ))),
            v => Err(anyhow!("Value {} is not supported", v)),
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
            "select a, b, c from {} where a=100 order by c asc limit 5 offset 10",
            url
        );
        let statement = &Parser::parse_sql(&TyrDialect::default(), sql.as_ref()).unwrap()[0];
        let sql: Sql = statement.try_into().unwrap();
        assert_eq!(sql.source, url);
        assert_eq!(sql.limit, Some(5));
        assert_eq!(sql.offset, Some(10));
        assert_eq!(sql.order_by, vec![("c".into(), true)]);
        assert_eq!(sql.selection, vec![col("a"), col("b"), col("c")]);
    }
}
