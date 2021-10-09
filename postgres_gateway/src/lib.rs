mod errors;

use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use deadpool_postgres::tokio_postgres::Column;
use deadpool_postgres::tokio_postgres::types::Type;
use pyo3::prelude::*;
use pyo3::{PyMappingProtocol, wrap_pyfunction};
use pyo3::exceptions::PyKeyError;
use pyo3::ffi::PyLongObject;
use pyo3::types::{PyInt, PyLong};
use tokio_postgres::NoTls;
use tokio_postgres::types::FromSql;
use std::os::raw::c_long;
use crate::errors::LibError;

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

struct Value {
    type_value: String,
    data: Vec<u8>,
}

impl<'a> FromSql<'a> for Value {
    fn from_sql(ty: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        Ok(Value {
            type_value: ty.name().to_string(),
            data: Vec::from(raw),
        })
    }

    fn accepts(ty: &Type) -> bool {
        true
    }
}

impl ToPyObject for Value {
    fn to_object(&self, py: Python) -> PyObject {
        match self.type_value.as_str() {
            "int2" => unsafe {
                i16::from_be_bytes([
                    *self.data.get_unchecked(0),
                    *self.data.get_unchecked(1),
                ]).to_object(py)
            },
            "int4" => unsafe {
                i32::from_be_bytes([
                    *self.data.get_unchecked(0),
                    *self.data.get_unchecked(1),
                    *self.data.get_unchecked(2),
                    *self.data.get_unchecked(3),
                ]).to_object(py)
            },
            "int8" => unsafe {
                i64::from_be_bytes([
                    *self.data.get_unchecked(0),
                    *self.data.get_unchecked(1),
                    *self.data.get_unchecked(2),
                    *self.data.get_unchecked(3),
                    *self.data.get_unchecked(4),
                    *self.data.get_unchecked(5),
                    *self.data.get_unchecked(6),
                    *self.data.get_unchecked(7),
                ]).to_object(py)
            },
            other => {
                println!("cannot infer {:?}", other);
                let v = vec![self.data.to_object(py), self.type_value.to_object(py)];
                v.to_object(py)
            },
        }
    }
}

#[pyproto]
impl PyMappingProtocol for Row {
    fn __getitem__(&self, key: PyObject) -> PyResult<PyObject> {
        Python::with_gil(|py| {
            match key.extract::<usize>(py) {
                Ok(ind) => Ok(self._row.get::<usize, Value>(ind).to_object(py)),
                _ => {
                    match key.extract::<String>(py) {
                        Ok(rust_key) => Python::with_gil(|py| {
                            match self._row
                                .columns()
                                .iter()
                                .enumerate()
                                .filter(|(ind, col)| col.name().to_string() == rust_key)
                                .next() {
                                None => Err(PyKeyError::new_err(key)),
                                Some((ind, _)) => Ok(self._row.get::<usize, Value>(ind).to_object(py)),
                            }
                        }),
                        Err(e) => Err(PyKeyError::new_err(key)),
                    }
                }
            }
        })
    }
}

impl Connection {
    async fn _execute(&self, query: String) -> Result<u64, LibError> {
        Ok(self.client.execute(query.as_str(), &[]).await.map_err(LibError::from)?)
    }

    async fn _fetchrow(&self, query: String) -> Result<Row, LibError> {
        let row = self.client.query_one(query.as_str(), &[]).await.map_err(LibError::from)?;
        println!("{:?}", row);
        Ok(Row { _row: row })
    }

    fn _close(&self) {
        self.handle.abort();
    }
}

#[pymethods]
impl Connection {
    fn execute<'p>(&self, query: String, py: Python<'p>) -> PyResult<&'p PyAny> {
        let slf = self.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            match slf._execute(query).await {
                Err(e) => Err(PyErr::from(e)),
                Ok(v) => Ok(Python::with_gil(|py| v.to_object(py)))
            }
        })
    }

    fn fetchrow<'p>(&self, query: String, py: Python<'p>) -> PyResult<&'p PyAny> {
        let slf = self.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            match slf._fetchrow(query).await {
                Err(e) => Err(PyErr::from(e)),
                Ok(v) => Ok(Python::with_gil(|py| v.into_py(py)))
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
            PyCell::new(py, Connection {
                client: Arc::new(client),
                handle: Arc::new(handle),
            }).unwrap().to_object(py)
        }))
    })
}

#[pymodule]
fn postgres_gateway(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_class::<Connection>()?;
    m.add_class::<Row>()?;
    m.add_function(wrap_pyfunction!(connect, m)?)?;
    Ok(())
}
