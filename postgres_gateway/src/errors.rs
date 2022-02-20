use pyo3::{self, create_exception, exceptions::PyException, PyErr};
use tokio_postgres;

pub struct LibError {
    pub code: i32,
    pub detail: Option<String>,
}

unsafe impl Send for LibError {}
unsafe impl Sync for LibError {}

create_exception!(postgres_gateway, DBError, PyException);
create_exception!(postgres_gateway, UniqueViolationError, DBError);

impl From<tokio_postgres::Error> for LibError {
    fn from(e: tokio_postgres::Error) -> Self {
        LibError {
            code: 1,
            detail: Some(format!("{:?}", e)),
        }
    }
}

impl From<LibError> for pyo3::PyErr {
    fn from(e: LibError) -> Self {
        pyo3::Python::with_gil(|py| {
            if e.code == 23505 {
                return UniqueViolationError::new_err(e.detail);
            }

            PyErr::from_instance(pyo3::exceptions::PyRuntimeError::new_err(e.detail).instance(py))
        })
    }
}
