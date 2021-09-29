mod errors;

use pyo3::prelude::*;
use pyo3::wrap_pyfunction;
use deadpool_postgres::{Config, Manager, ManagerConfig, Pool, RecyclingMethod};
use tokio_postgres::NoTls;
use crate::errors::LibError;

#[pyclass]
struct PGGateway {
    dsn: String,
}

#[pyclass]
struct Connection {
    dsn: String,
}

#[pyfunction]
fn connect(py: Python, dsn: String) -> PyResult<&PyAny> {
    pyo3_asyncio::tokio::future_into_py(py, async move {
        let (client, connection) = tokio_postgres::connect(dsn.as_ref(), NoTls)
            .await
            .map_err(LibError::from)?;
        Ok(Python::with_gil(|py| PyCell::new(py, Connection { dsn }).unwrap().to_object(py)))
    })
}

#[pymodule]
fn pet_farm(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_class::<PGGateway>()?;
    m.add_function(wrap_pyfunction!(connect, m)?)?;
    Ok(())
}
