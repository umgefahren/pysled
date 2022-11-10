use std::path::PathBuf;

use pyo3::{exceptions::PyValueError, prelude::*};
use sled::{Db, Tree};

fn convert_to_pyresult<T>(inp: sled::Result<T>) -> PyResult<T> {
    inp.map_err(|e| PyValueError::new_err(e.to_string()))
}

#[pyclass]
pub struct CompareAndSwapError {
    #[pyo3(get, set)]
    pub current: Option<Vec<u8>>,
    #[pyo3(get, set)]
    pub proposed: Option<Vec<u8>>,
}

#[pyclass]
pub struct SledDb {
    inner: Db,
}

#[pymethods]
impl SledDb {
    #[new]
    pub fn new(path: PathBuf) -> PyResult<Self> {
        let inner = sled::open(&path)
            .map_err(|e| PyValueError::new_err(format!("Failed to open db: {}", e.to_string())))?;
        Ok(Self { inner })
    }

    pub fn insert(&self, key: &[u8], value: Vec<u8>) -> PyResult<Option<Vec<u8>>> {
        convert_to_pyresult(self.inner.insert(key, value)).map(|o| o.map(|i| i.to_vec()))
    }

    pub fn get(&self, key: &[u8]) -> PyResult<Option<Vec<u8>>> {
        convert_to_pyresult(self.inner.get(key)).map(|o| o.map(|i| i.to_vec()))
    }

    pub fn remove(&self, key: &[u8]) -> PyResult<Option<Vec<u8>>> {
        convert_to_pyresult(self.inner.remove(key)).map(|o| o.map(|i| i.to_vec()))
    }

    pub fn clear(&self) -> PyResult<()> {
        convert_to_pyresult(self.inner.clear())
    }

    pub fn all(&self) -> PyResult<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut out = Vec::new();
        let iter = self.inner.iter();
        out.reserve(iter.size_hint().0);
        for e in iter {
            let (a, b) = convert_to_pyresult(e)?;
            out.push((a.to_vec(), b.to_vec()));
        }
        Ok(out)
    }

    pub fn compare_and_swamp(
        &self,
        key: &[u8],
        old: Option<&[u8]>,
        new: Option<Vec<u8>>,
    ) -> PyResult<Option<CompareAndSwapError>> {
        convert_to_pyresult(self.inner.compare_and_swap(key, old, new)).map(|e| {
            e.map_err(|i| CompareAndSwapError {
                current: i.current.map(|e| e.to_vec()),
                proposed: i.proposed.map(|e| e.to_vec()),
            })
            .err()
        })
    }

    pub fn checksum(&self) -> PyResult<u32> {
        convert_to_pyresult(self.inner.checksum())
    }

    pub fn flush(&self) -> PyResult<usize> {
        convert_to_pyresult(self.inner.flush())
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn __len__(&self) -> usize {
        self.inner.len()
    }

    pub fn __contains__(&self, key: &[u8]) -> PyResult<bool> {
        convert_to_pyresult(self.inner.contains_key(key))
    }

    pub fn __getitem__(&self, key: &[u8]) -> PyResult<Option<Vec<u8>>> {
        self.get(key)
    }

    pub fn __setitem__(&self, key: &[u8], value: Vec<u8>) -> PyResult<()> {
        self.insert(key, value).map(|_| ())
    }

    pub fn __delitem__(&self, key: &[u8]) -> PyResult<()> {
        self.remove(key).map(|_| ())
    }

    #[getter]
    pub fn name(&self) -> Vec<u8> {
        self.inner.name().to_vec()
    }

    pub fn contains_key(&self, key: &[u8]) -> PyResult<bool> {
        convert_to_pyresult(self.inner.contains_key(key))
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn open_tree(&self, name: &[u8]) -> PyResult<SledTree> {
        convert_to_pyresult(self.inner.open_tree(name)).map(|e| SledTree { inner: e })
    }

    pub fn drop_tree(&self, name: &[u8]) -> PyResult<bool> {
        convert_to_pyresult(self.inner.drop_tree(name))
    }

    pub fn size_on_disk(&self) -> PyResult<u64> {
        convert_to_pyresult(self.inner.size_on_disk())
    }
}

#[pyclass(mapping)]
pub struct SledTree {
    inner: Tree,
}

#[pymethods]
impl SledTree {
    pub fn insert(&self, key: &[u8], value: Vec<u8>) -> PyResult<Option<Vec<u8>>> {
        convert_to_pyresult(self.inner.insert(key, value)).map(|o| o.map(|i| i.to_vec()))
    }

    pub fn get(&self, key: &[u8]) -> PyResult<Option<Vec<u8>>> {
        convert_to_pyresult(self.inner.get(key)).map(|o| o.map(|i| i.to_vec()))
    }

    pub fn remove(&self, key: &[u8]) -> PyResult<Option<Vec<u8>>> {
        convert_to_pyresult(self.inner.remove(key)).map(|o| o.map(|i| i.to_vec()))
    }

    pub fn clear(&self) -> PyResult<()> {
        convert_to_pyresult(self.inner.clear())
    }

    pub fn all(&self) -> PyResult<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut out = Vec::new();
        let iter = self.inner.iter();
        out.reserve(iter.size_hint().0);
        for e in iter {
            let (a, b) = convert_to_pyresult(e)?;
            out.push((a.to_vec(), b.to_vec()));
        }
        Ok(out)
    }

    pub fn compare_and_swamp(
        &self,
        key: &[u8],
        old: Option<&[u8]>,
        new: Option<Vec<u8>>,
    ) -> PyResult<Option<CompareAndSwapError>> {
        convert_to_pyresult(self.inner.compare_and_swap(key, old, new)).map(|e| {
            e.map_err(|i| CompareAndSwapError {
                current: i.current.map(|e| e.to_vec()),
                proposed: i.proposed.map(|e| e.to_vec()),
            })
            .err()
        })
    }

    pub fn checksum(&self) -> PyResult<u32> {
        convert_to_pyresult(self.inner.checksum())
    }

    pub fn flush(&self) -> PyResult<usize> {
        convert_to_pyresult(self.inner.flush())
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn __len__(&self) -> usize {
        self.inner.len()
    }

    pub fn __contains__(&self, key: &[u8]) -> PyResult<bool> {
        convert_to_pyresult(self.inner.contains_key(key))
    }

    pub fn __getitem__(&self, key: &[u8]) -> PyResult<Option<Vec<u8>>> {
        self.get(key)
    }

    pub fn __setitem__(&self, key: &[u8], value: Vec<u8>) -> PyResult<()> {
        self.insert(key, value).map(|_| ())
    }

    pub fn __delitem__(&self, key: &[u8]) -> PyResult<()> {
        self.remove(key).map(|_| ())
    }

    #[getter]
    pub fn name(&self) -> Vec<u8> {
        self.inner.name().to_vec()
    }
}

/// Formats the sum of two numbers as string.
#[pyfunction]
fn sum_as_string(a: usize, b: usize) -> PyResult<String> {
    Ok((a + b).to_string())
}

/// A Python module implemented in Rust.
#[pymodule]
fn pysled(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<SledDb>()?;
    m.add_class::<SledTree>()?;
    m.add_function(wrap_pyfunction!(sum_as_string, m)?)?;
    Ok(())
}
