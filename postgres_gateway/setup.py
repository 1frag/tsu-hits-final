from setuptools import setup
from setuptools_rust import RustExtension

setup(
    name="postgres_gateway",
    version="0.0.1",
    classifiers=[
        "Programming Language :: Python",
        "Programming Language :: Rust",
    ],
    rust_extensions=[
        RustExtension('postgres_gateway', "Cargo.toml", debug=False),
    ],
    include_package_data=True,
    zip_safe=False,
)
