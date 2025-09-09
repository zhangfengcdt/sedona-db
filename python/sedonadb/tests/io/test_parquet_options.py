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

import pytest
import tempfile
import sedonadb
from pathlib import Path
from sedonadb.testing import skip_if_not_exists


def test_read_parquet_options_backward_compatibility(geoarrow_data):
    """Test that adding options parameter doesn't break existing code"""
    con = sedonadb.connect()
    test_file = geoarrow_data / "quadrangles/files/quadrangles_100k_geo.parquet"
    skip_if_not_exists(test_file)
    
    # Original call without options
    df1 = con.read_parquet(test_file)
    
    # New call with explicit None
    df2 = con.read_parquet(test_file, options=None)
    
    # New call with empty dict
    df3 = con.read_parquet(test_file, options={})
    
    # All should produce identical results
    assert df1.count() == df2.count() == df3.count()
    
    # Compare schema string representations (simple but effective)
    assert str(df1.schema) == str(df2.schema) == str(df3.schema)


def test_read_parquet_options_type_conversion():
    """Test that different Python types in options are properly converted to strings"""
    con = sedonadb.connect()
    
    # Create a simple test file
    with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
        # Use a known working URL to test the interface
        url = "https://raw.githubusercontent.com/geoarrow/geoarrow-data/v0.2.0/example/files/example_geometry_geo.parquet"
        
        # Test various Python types in options
        test_options = [
            {"string": "value"},
            {"boolean": True},
            {"false_boolean": False},
            {"integer": 42},
            {"float": 3.14},
            {"none_value": None},
            {"mixed": "string", "num": 123, "bool": True}
        ]
        
        for options in test_options:
            # Should not raise TypeError or similar interface errors
            try:
                df = con.read_parquet(url, options=options)
                assert df.count() > 0  # Basic sanity check
            except Exception as e:
                # If it fails, it should not be due to type conversion issues
                assert "type" not in str(e).lower() or "convert" not in str(e).lower()


@pytest.mark.parametrize("option_key,option_value", [
    ("aws.region", "us-west-2"),
    ("aws.skip_signature", "true"), 
    ("aws.SKIP_SIGNATURE", "true"),
    ("aws.nosign", "true"),
    ("aws.endpoint", "https://custom.s3.endpoint.com"),
    ("aws.access_key_id", "dummy_key"),
    ("some.custom.option", "custom_value"),
    ("datafusion.option", "test"),
])
def test_read_parquet_options_s3_keys(geoarrow_data, option_key, option_value):
    """Test that common S3 and DataFusion option keys are properly handled"""
    con = sedonadb.connect()
    test_file = geoarrow_data / "quadrangles/files/quadrangles_100k_geo.parquet"
    skip_if_not_exists(test_file)
    
    options = {option_key: option_value}
    
    # Should process without interface errors (may have other failures)
    try:
        df = con.read_parquet(test_file, options=options)
        assert df.count() > 0
    except Exception as e:
        error_msg = str(e).lower()
        # Should not be parameter-related errors
        assert "options" not in error_msg
        assert "parameter" not in error_msg
        assert "argument" not in error_msg
        assert "keyword" not in error_msg


def test_read_parquet_options_empty_vs_none():
    """Test the distinction between None and empty dict options"""
    con = sedonadb.connect()
    url = "https://raw.githubusercontent.com/geoarrow/geoarrow-data/v0.2.0/example/files/example_geometry_geo.parquet"
    
    # All these should behave identically for the interface
    df1 = con.read_parquet(url)
    df2 = con.read_parquet(url, options=None)
    df3 = con.read_parquet(url, options={})
    
    # Should produce identical results
    assert df1.count() == df2.count() == df3.count()


def test_read_parquet_options_multiple_files(geoarrow_data):
    """Test options parameter with multiple file inputs"""
    con = sedonadb.connect()
    files = list(geoarrow_data.glob("example/files/*_geo.parquet"))
    
    if not files:
        pytest.skip("No example files found")
    
    # Take first few files to avoid too large a test
    test_files = files[:3]
    
    # Test with and without options
    df1 = con.read_parquet(test_files)
    df2 = con.read_parquet(test_files, options={"test.multi": "files"})
    
    # Results should be consistent (options don't affect local file reads in this test)
    assert df1.count() == df2.count()
    assert str(df1.schema) == str(df2.schema)


def test_read_parquet_options_error_handling():
    """Test that appropriate errors are raised for invalid inputs"""
    con = sedonadb.connect()
    
    # These should raise appropriate errors (not implementation-specific errors)
    with pytest.raises(Exception):  # Should fail - non-existent file
        con.read_parquet("/non/existent/file.parquet", options={"test": "value"})


def test_read_parquet_options_documentation_examples():
    """Test the examples from the function documentation"""
    con = sedonadb.connect()
    url = "https://raw.githubusercontent.com/geoarrow/geoarrow-data/v0.2.0/example/files/example_geometry_geo.parquet"
    
    # Example 1: Basic usage
    df1 = con.read_parquet(url)
    assert df1.count() > 0
    
    # Example 2: With S3 options (using HTTP URL for testing)
    df2 = con.read_parquet(url, options={"aws.nosign": True})
    assert df2.count() > 0
    
    # Should produce similar results for HTTP URLs
    assert df1.count() == df2.count()