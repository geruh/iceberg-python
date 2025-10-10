# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import os
from setuptools import setup, Extension

# Check if we're in cibuildwheel - if so, Cython is required
in_cibuildwheel = os.environ.get("CIBUILDWHEEL", "0") == "1"

try:
    import Cython.Compiler.Options
    from Cython.Build import cythonize
    
    # Enable annotation for Cython (generates .html files for analysis)
    Cython.Compiler.Options.annotate = True
    
    # Determine compiler flags based on platform
    if os.name == "nt":  # Windows
        extra_compile_args = ["/O2"]
    else:  # UNIX-based systems (Linux, macOS)
        extra_compile_args = ["-O3"]
    
    package_path = "pyiceberg"
    
    extensions = [
        Extension(
            "pyiceberg.avro.decoder_fast",
            [os.path.join(package_path, "avro", "decoder_fast.pyx")],
            extra_compile_args=extra_compile_args,
            language="c",
        )
    ]
    
    # Cythonize with the same options as the old build-module.py
    ext_modules = cythonize(
        extensions,
        include_path=[package_path],  # Match old include_path
        compiler_directives={"language_level": "3"},
        annotate=True,
    )
    
except ImportError:
    if in_cibuildwheel:
        # In cibuildwheel, Cython is required
        raise
    # If Cython is not available and we're not in cibuildwheel,
    # build without extensions (matches allowed_to_fail logic)
    ext_modules = []

setup(
    ext_modules=ext_modules,
)
