mod errors;

use std::sync::Arc;
use pyo3::prelude::*;
use pyo3::wrap_pyfunction;
use tokio_postgres::NoTls;
use crate::errors::LibError;

#[pyclass]
#[derive(Clone)]
struct Connection {
    client: Arc<tokio_postgres::Client>,
    handle: Arc<tokio::task::JoinHandle<()>>,
}

impl Connection {
    async fn _execute(&self, query: String) -> Result<u64, LibError> {
        Ok(self.client.execute(query.as_str(), &[]).await.map_err(LibError::from)?)
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
    m.add_function(wrap_pyfunction!(connect, m)?)?;
    Ok(())
}
