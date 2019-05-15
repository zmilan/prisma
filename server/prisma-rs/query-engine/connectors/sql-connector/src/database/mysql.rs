use crate::{
    error::SqlError, DatabaseType, MutationBuilder, RawQuery, SqlId, SqlResult, SqlRow, ToSqlRow, Transaction,
    Transactional,
};
use connector::{error::*, ConnectorResult};
use mysql_client as my;
use prisma_common::config::{ConnectionLimit, ConnectionStringConfig, ExplicitConfig, PrismaDatabase};
use prisma_models::{GraphqlId, PrismaValue, ProjectRef, TypeIdentifier};
use prisma_query::{
    ast::*,
    visitor::{self, Visitor},
};
use serde_json::{Map, Number, Value};
use std::convert::TryFrom;

/// The World's Most Advanced Open Source Relational Database
pub struct Mysql {
    pool: my::Pool,
}

impl TryFrom<&PrismaDatabase> for Mysql {
    type Error = ConnectorError;

    fn try_from(db: &PrismaDatabase) -> ConnectorResult<Self> {
        match db {
            PrismaDatabase::ConnectionString(ref config) => Ok(Mysql::try_from(config)?),
            PrismaDatabase::Explicit(ref config) => Ok(Mysql::try_from(config)?),
            _ => Err(ConnectorError::DatabaseCreationError(
                "Could not understand the configuration format.",
            )),
        }
    }
}

impl TryFrom<&ExplicitConfig> for Mysql {
    type Error = SqlError;

    fn try_from(e: &ExplicitConfig) -> SqlResult<Self> {
        let db_name = e.database.as_ref().map(|x| x.as_str()).unwrap_or("mysql");

        let mut builder = my::OptsBuilder::new();

        builder.ip_or_hostname(Some(e.host.as_ref()));
        builder.tcp_port(e.port);
        builder.user(Some(e.user.as_ref()));
        builder.db_name(Some(db_name));
        builder.pass(e.password.as_ref().map(|p| p.as_str()));
        builder.ssl_opts(Some(("", None::<(String, String)>)));
        builder.verify_peer(false);

        let opts = my::Opts::from(builder);
        let pool = my::Pool::new_manual(1, e.limit() as usize, opts)?;

        Ok(Self { pool })
    }
}

impl TryFrom<&ConnectionStringConfig> for Mysql {
    type Error = SqlError;

    fn try_from(s: &ConnectionStringConfig) -> SqlResult<Self> {
        let db_name = s.database.as_ref().map(|x| x.as_str()).unwrap_or("mysql");
        let mut builder = my::OptsBuilder::new();

        builder.ip_or_hostname(s.uri.host_str());
        builder.tcp_port(s.uri.port().unwrap_or(3306));
        builder.user(Some(s.uri.username()));
        builder.db_name(Some(db_name));
        builder.pass(s.uri.password());
        builder.ssl_opts(Some(("", None::<(String, String)>)));
        builder.verify_peer(false);

        let opts = my::Opts::from(builder);
        let pool = my::Pool::new_manual(1, s.limit() as usize, opts)?;

        Ok(Self { pool })
    }
}

impl Transactional for Mysql {
    const DATABASE_TYPE: DatabaseType = DatabaseType::Mysql;

    fn with_transaction<F, T>(&self, _: &str, f: F) -> SqlResult<T>
    where
        F: FnOnce(&mut Transaction) -> SqlResult<T>,
    {
        self.with_conn(|conn| {
            let mut tx = conn.start_transaction(true, None, None)?;
            let result = f(&mut tx);

            if result.is_ok() {
                tx.commit()?;
            }

            result
        })
    }
}

impl<'a> Transaction for my::Transaction<'a> {
    fn write(&mut self, q: Query) -> SqlResult<Option<GraphqlId>> {
        let (sql, params) = dbg!(visitor::Mysql::build(q));

        let mut stmt = self.prepare(&sql)?;
        let result = stmt.execute(params)?;

        Ok(Some(GraphqlId::from(result.last_insert_id())))
    }

    fn filter(&mut self, q: Select, idents: &[TypeIdentifier]) -> SqlResult<Vec<SqlRow>> {
        let (sql, params) = dbg!(visitor::Mysql::build(q));

        let mut stmt = self.prepare(&sql)?;
        let rows = stmt.execute(params)?;
        let mut result = Vec::new();

        for row in rows {
            result.push(row?.to_prisma_row(idents)?);
        }

        Ok(result)
    }

    fn truncate(&mut self, project: ProjectRef) -> SqlResult<()> {
        self.write(Query::from("SET FOREIGN_KEY_CHECKS=0"))?;

        for delete in MutationBuilder::truncate_tables(project) {
            if let Err(e) = self.delete(delete) {
                self.write(Query::from("SET FOREIGN_KEY_CHECKS=1"))?;
                return Err(e);
            }
        }

        self.write(Query::from("SET FOREIGN_KEY_CHECKS=1"))?;

        Ok(())
    }

    fn raw(&mut self, q: RawQuery) -> SqlResult<Value> {
        unimplemented!()
    }
}

impl ToSqlRow for my::Row {
    fn to_prisma_row<'b, T>(&'b self, idents: T) -> SqlResult<SqlRow>
    where
        T: IntoIterator<Item = &'b TypeIdentifier>,
    {
        fn convert(row: &my::Row, i: usize, typid: &TypeIdentifier) -> SqlResult<PrismaValue> {
            let result = match typid {
                TypeIdentifier::String => match row.get_opt(i)? {
                    Some(val) => PrismaValue::String(val),
                    None => PrismaValue::Null,
                },
                TypeIdentifier::GraphQLID | TypeIdentifier::Relation => match row.get_opt(i)? {
                    Some(val) => {
                        let id: SqlId = val;
                        PrismaValue::GraphqlId(GraphqlId::from(id))
                    }
                    None => PrismaValue::Null,
                },
                TypeIdentifier::Float => match row.get_opt(i)? {
                    Some(val) => PrismaValue::Float(val),
                    None => PrismaValue::Null,
                },
                TypeIdentifier::Int => match row.get_opt(i)? {
                    Some(val) => PrismaValue::Int(val),
                    None => PrismaValue::Null,
                },
                TypeIdentifier::Boolean => match row.get_opt(i)? {
                    Some(val) => PrismaValue::Boolean(val),
                    None => PrismaValue::Null,
                },
                TypeIdentifier::Enum => match row.get_opt(i)? {
                    Some(val) => PrismaValue::Enum(val),
                    None => PrismaValue::Null,
                },
                TypeIdentifier::Json => match row.get_opt(i)? {
                    Some(val) => serde_json::from_str(val)
                        .map(|r| PrismaValue::Json(r))
                        .map_err(|err| my::error::Error::FromValueError(row.as_ref(i).unwrap().clone())),
                    None => PrismaValue::Null,
                },
                TypeIdentifier::UUID => match row.get_opt(i)? {
                    Some(val) => Uuid::from_slice,
                    None => PrismaValue::Null,
                },
                TypeIdentifier::UUID => match row.try_get(i)? {
                    Some(val) => PrismaValue::Uuid(val),
                    None => PrismaValue::Null,
                },
                TypeIdentifier::DateTime => match row.try_get(i)? {
                    Some(val) => {
                        let ts: NaiveDateTime = val;
                        PrismaValue::DateTime(DateTime::<Utc>::from_utc(ts, Utc))
                    }
                    None => PrismaValue::Null,
                },
            };

            Ok(result)
        }

        let mut row = SqlRow::default();

        for (i, typid) in idents.into_iter().enumerate() {
            row.values.push(convert(self, i, typid)?);
        }

        Ok(row)
    }
}

impl Mysql {
    fn with_conn<F, T>(&self, f: F) -> SqlResult<T>
    where
        F: FnOnce(&mut my::PooledConn) -> SqlResult<T>,
    {
        let mut conn = self.pool.get_conn()?;
        let result = f(&mut conn);
        result
    }
}
