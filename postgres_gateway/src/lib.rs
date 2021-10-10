mod errors;

use crate::errors::LibError;
use pyo3::{exceptions::PyKeyError, prelude::*, wrap_pyfunction, PyMappingProtocol};
use std::{ops::Index, sync::Arc};
use tokio_postgres::NoTls;

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

fn adapt(py: Python, row: &tokio_postgres::Row, ind: usize) -> PyObject {
    match row.columns().index(ind).type_().name() {
        "int2" => row.get::<_, i16>(ind).to_object(py),
        "int4" => row.get::<_, i32>(ind).to_object(py),
        "int8" => row.get::<_, i64>(ind).to_object(py),
        "text" => row.get::<_, String>(ind).to_object(py),
        "bool" => row.get::<_, bool>(ind).to_object(py),
        other => {
            println!("{:?}", other);
            todo!()
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

    async fn _fetchrow(&self, query: String) -> Result<Row, LibError> {
        let row = self
            .client
            .query_one(query.as_str(), &[])
            .await
            .map_err(LibError::from)?;
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
                Ok(v) => Ok(Python::with_gil(|py| v.to_object(py))),
            }
        })
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
fn postgres_gateway(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_class::<Connection>()?;
    m.add_class::<Row>()?;
    m.add_function(wrap_pyfunction!(connect, m)?)?;
    Ok(())
}
