use tokio_postgres;
use pyo3;
use pyo3::PyErr;

pub struct LibError {
    code: i32,
    detail: String,
}

impl From<tokio_postgres::Error> for LibError {
    fn from(e: tokio_postgres::Error) -> Self {
        LibError {code: 1, detail: format!("{:?}", e)}
    }
}

impl From<LibError> for pyo3::PyErr {
    fn from(e: LibError) -> Self {
        pyo3::Python::with_gil(|py| {
            PyErr::from_instance(pyo3::exceptions::PyRuntimeError::new_err(e.detail).instance(py))
        })
    }
}
