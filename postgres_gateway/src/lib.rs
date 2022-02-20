mod errors;

use crate::errors::LibError;
use byteorder::{BigEndian, ReadBytesExt};
use deadpool_postgres::tokio_postgres::types::Type;
use once_cell::sync::OnceCell;
use postgres_types::FromSql;
use pyo3::{
    exceptions::PyKeyError, prelude::*, types::PyTuple, wrap_pyfunction, PyMappingProtocol,
};
use serde_json::Value;
use std::{collections::HashMap, error::Error, ops::Index, sync::Arc};
use tokio_postgres::NoTls;

#[derive(Clone)]
pub struct Config {
    pub uuid_class: PyObject,
    pub setattr: PyObject,
    pub safe_unknown: PyObject,
}

static mut CONFIG: OnceCell<Config> = OnceCell::new();

#[pyclass]
#[derive(Clone)]
struct Connection {
    client: Arc<tokio_postgres::Client>,
    handle: Arc<tokio::task::JoinHandle<()>>,
}

#[pyclass]
struct Row {
    _row: tokio_postgres::Row,
}

#[pymethods]
impl Row {
    fn keys(&self, py: Python) -> PyObject {
        self._row
            .columns()
            .iter()
            .map(|col| col.name().to_string())
            .collect::<Vec<String>>()
            .to_object(py)
    }
}

struct Nullable(bool);

impl<'a> FromSql<'a> for Nullable {
    #[inline]
    fn from_sql(_ty: &Type, _raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        Ok(Nullable(false))
    }
    #[inline]
    fn from_sql_null(_ty: &Type) -> Result<Self, Box<dyn Error + Sync + Send>> {
        Ok(Nullable(true))
    }
    #[inline]
    fn accepts(_ty: &Type) -> bool {
        true
    }
}

struct AnyType(Vec<u8>);

impl<'a> FromSql<'a> for AnyType {
    #[inline]
    fn from_sql(_ty: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        Ok(AnyType(raw.to_vec()))
    }
    #[inline]
    fn accepts(_ty: &Type) -> bool {
        true
    }
}

struct PyUUID(u128);

impl<'a> FromSql<'a> for PyUUID {
    #[inline]
    fn from_sql(_ty: &Type, mut raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        let v = raw.read_u128::<BigEndian>()?;
        Ok(PyUUID(v))
    }
    #[inline]
    fn accepts(_ty: &Type) -> bool {
        true
    }
}

impl ToPyObject for PyUUID {
    fn to_object(&self, py: Python) -> PyObject {
        let cfg = unsafe { CONFIG.get() }.unwrap();
        let obj = cfg
            .uuid_class
            .getattr(py, "__new__")
            .unwrap()
            .call1(py, (&cfg.uuid_class,))
            .unwrap();
        cfg.setattr
            .call1(py, (&obj, "int", &self.0.to_object(py)))
            .unwrap();
        cfg.setattr
            .call1(py, (&obj, "is_safe", &cfg.safe_unknown))
            .unwrap();
        obj
    }
}

#[pyfunction]
fn create_uuid(py: Python, int: u128) -> PyObject {
    PyUUID(int).to_object(py)
}

struct PyJson(serde_json::Value);

impl ToPyObject for PyJson {
    fn to_object(&self, py: Python) -> PyObject {
        match &self.0 {
            Value::Null => py.None(),
            Value::Bool(x) => x.to_object(py),
            Value::Number(x) => {
                if x.is_u64() {
                    x.as_u64().to_object(py)
                } else if x.is_i64() {
                    x.as_i64().to_object(py)
                } else {
                    x.as_f64().to_object(py)
                }
            }
            Value::String(x) => x.to_object(py),
            Value::Array(x) => x
                .iter()
                .map(|x| PyJson(x.clone()).to_object(py))
                .collect::<Vec<PyObject>>()
                .to_object(py),
            Value::Object(x) => x
                .iter()
                .map(|(key, value)| (key.to_string(), PyJson(value.clone()).to_object(py)))
                .collect::<HashMap<String, PyObject>>()
                .to_object(py),
        }
    }
}

fn adapt(py: Python, row: &tokio_postgres::Row, ind: usize) -> PyObject {
    if row.get::<_, Nullable>(ind).0 {
        return py.None();
    }
    match row.columns().index(ind).type_().name() {
        "int2" => row.get::<_, i16>(ind).to_object(py),
        "_int2" => row.get::<_, Vec<i16>>(ind).to_object(py),
        "int4" => row.get::<_, i32>(ind).to_object(py),
        "_int4" => row.get::<_, Vec<i32>>(ind).to_object(py),
        "int8" => row.get::<_, i64>(ind).to_object(py),
        "_int8" => row.get::<_, Vec<i64>>(ind).to_object(py),
        "text" => row.get::<_, String>(ind).to_object(py),
        "_text" => row.get::<_, Vec<String>>(ind).to_object(py),
        "varchar" => row.get::<_, String>(ind).to_object(py),
        "_varchar" => row.get::<_, Vec<String>>(ind).to_object(py),
        "char" => row.get::<_, String>(ind).to_object(py),
        "_char" => row.get::<_, Vec<String>>(ind).to_object(py),
        "bpchar" => row.get::<_, String>(ind).to_object(py),
        "_bpchar" => row.get::<_, Vec<String>>(ind).to_object(py),
        "bool" => row.get::<_, bool>(ind).to_object(py),
        "_bool" => row.get::<_, Vec<bool>>(ind).to_object(py),
        "uuid" => row.get::<_, PyUUID>(ind).to_object(py),
        "_uuid" => row.get::<_, Vec<PyUUID>>(ind).to_object(py),
        "json" | "jsonb" => PyJson(row.get::<_, serde_json::Value>(ind)).to_object(py),
        "_json" | "_jsonb" => {
            let lst = row.get::<_, Vec<serde_json::Value>>(ind);
            lst.iter()
                .map(|jsn| PyJson(jsn.clone()).to_object(py))
                .collect::<Vec<PyObject>>()
                .to_object(py)
        }
        other => {
            let any_value = row.get::<_, AnyType>(ind).0;
            println!("{:?} {:?}", other, any_value);
            any_value.to_object(py)
        }
    }
}

#[pyproto]
impl PyMappingProtocol for Row {
    fn __getitem__(&self, key: PyObject) -> PyResult<PyObject> {
        Python::with_gil(|py| match key.extract::<usize>(py) {
            Ok(ind) => Ok(adapt(py, &self._row, ind)),
            _ => match key.extract::<String>(py) {
                Ok(rust_key) => Python::with_gil(|py| {
                    match self
                        ._row
                        .columns()
                        .iter()
                        .enumerate()
                        .filter(|(_, col)| col.name().to_string() == rust_key)
                        .next()
                    {
                        None => Err(PyKeyError::new_err(key)),
                        Some((ind, _)) => Ok(adapt(py, &self._row, ind)),
                    }
                }),
                Err(_) => Err(PyKeyError::new_err(key)),
            },
        })
    }
}

impl Connection {
    async fn _execute(&self, query: String) -> Result<u64, LibError> {
        Ok(self
            .client
            .execute(query.as_str(), &[])
            .await
            .map_err(LibError::from)?)
    }

    async fn _simple_execute(&self, query: String) -> Result<u64, LibError> {
        match self.client.simple_query(query.as_str()).await {
            Ok(res) => Ok(res.len() as u64),
            Err(e) => match e.as_db_error() {
                None => {
                    println!("Unknown error: {}", e);
                    return Err(LibError::from(e));
                }
                Some(t) => match t.code().code() {
                    "23505" => {
                        return Err(LibError {
                            code: 23505,
                            detail: t.constraint().map(|c| c.to_string()),
                        })
                    }
                    _ => {
                        println!("Unknown error: {} code={}", e, t.code().code());
                        return Err(LibError::from(e));
                    }
                },
            },
        }
    }

    async fn _fetchrow(&self, query: String) -> Result<Row, LibError> {
        let row = self
            .client
            .query_one(query.as_str(), &[])
            .await
            .map_err(LibError::from)?;
        Ok(Row { _row: row })
    }

    fn _close(&self) {
        self.handle.abort();
    }
}

#[pymethods]
impl Connection {
    #[args(params = "*")]
    fn execute<'p>(&self, query: String, params: &PyTuple, py: Python<'p>) -> PyResult<&'p PyAny> {
        let slf = self.clone();
        if params.len() > 0 {
            pyo3_asyncio::tokio::future_into_py(py, async move {
                match slf._execute(query).await {
                    Err(e) => Err(PyErr::from(e)),
                    Ok(v) => Ok(Python::with_gil(|py| v.to_object(py))),
                }
            })
        } else {
            pyo3_asyncio::tokio::future_into_py(py, async move {
                match slf._simple_execute(query).await {
                    Err(e) => Err(PyErr::from(e)),
                    Ok(v) => Ok(Python::with_gil(|py| v.to_object(py))),
                }
            })
        }
    }

    fn fetchrow<'p>(&self, query: String, py: Python<'p>) -> PyResult<&'p PyAny> {
        let slf = self.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            match slf._fetchrow(query).await {
                Err(e) => Err(PyErr::from(e)),
                Ok(v) => Ok(Python::with_gil(|py| v.into_py(py))),
            }
        })
    }

    fn close(&self) {
        self.handle.abort();
    }
}

#[pyfunction]
fn connect(py: Python, dsn: String) -> PyResult<&PyAny> {
    pyo3_asyncio::tokio::future_into_py(py, async move {
        let (client, connection) = tokio_postgres::connect(dsn.as_ref(), NoTls)
            .await
            .map_err(LibError::from)?;
        let handle = tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });
        Ok(Python::with_gil(|py| {
            PyCell::new(
                py,
                Connection {
                    client: Arc::new(client),
                    handle: Arc::new(handle),
                },
            )
            .unwrap()
            .to_object(py)
        }))
    })
}

#[pymodule]
fn postgres_gateway(py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_class::<Connection>()?;
    m.add_class::<Row>()?;

    m.add_function(wrap_pyfunction!(connect, m)?)?;
    m.add_function(wrap_pyfunction!(create_uuid, m)?)?;

    m.add(
        "UniqueViolationError",
        py.get_type::<errors::UniqueViolationError>(),
    )?;

    let uuid_module = py.import("uuid")?;
    let uuid_class = uuid_module.getattr("UUID")?.to_object(py);
    let setattr = py.eval("object.__setattr__", None, None)?.to_object(py);
    let safe_unknown = uuid_module
        .getattr("SafeUUID")?
        .getattr("unknown")?
        .to_object(py);
    unsafe {
        assert!(CONFIG
            .set(Config {
                uuid_class,
                setattr,
                safe_unknown
            })
            .is_ok());
    }
    Ok(())
}
