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
import geoarrow.pyarrow as ga  # noqa: F401
import pyarrow as pa
import pytest
import sedonadb


def test_read_parquet(con, geoarrow_data):
    # Check one file
    tab = con.read_parquet(
        geoarrow_data / "quadrangles/files/quadrangles_100k_geo.parquet"
    ).to_arrow_table()
    assert tab["geometry"].type.extension_name == "geoarrow.wkb"

    # Check many files
    tab = con.read_parquet(
        geoarrow_data.glob("example/files/*_geo.parquet")
    ).to_arrow_table()
    assert tab["geometry"].type.extension_name == "geoarrow.wkb"
    assert len(tab) == 244


def test_read_parquet_local_glob(con, geoarrow_data):
    # The above test uses .glob() method, this test uses the raw string
    tab = con.read_parquet(
        geoarrow_data / "example/files/*_geo.parquet"
    ).to_arrow_table()
    assert tab["geometry"].type.extension_name == "geoarrow.wkb"
    assert len(tab) == 244

    tab = con.read_parquet(
        geoarrow_data / "example/files/example_polygon-*geo.parquet"
    ).to_arrow_table()
    assert len(tab) == 12


def test_read_parquet_error(con):
    with pytest.raises(sedonadb._lib.SedonaError, match="No table paths were provided"):
        con.read_parquet([])


def test_dynamic_object_stores():
    # We need a fresh connection here to make sure other autoregistration
    # doesn't affect the ability of read_parquet() to use an object store
    con = sedonadb.connect()
    url = "https://raw.githubusercontent.com/geoarrow/geoarrow-data/v0.2.0/example/files/example_geometry_geo.parquet"

    schema = pa.schema(con.read_parquet(url))
    assert schema.field("geometry").type.extension_name == "geoarrow.wkb"


def test_read_parquet_options_parameter(con, geoarrow_data):
    """Test the options parameter functionality for read_parquet()"""
    test_file = geoarrow_data / "quadrangles/files/quadrangles_100k_geo.parquet"

    # Test 1: Backward compatibility - no options parameter
    tab1 = con.read_parquet(test_file).to_arrow_table()
    assert tab1["geometry"].type.extension_name == "geoarrow.wkb"

    # Test 2: options=None (explicit None)
    tab2 = con.read_parquet(test_file, options=None).to_arrow_table()
    assert tab2["geometry"].type.extension_name == "geoarrow.wkb"
    assert len(tab2) == len(tab1)  # Should be identical

    # Test 3: Empty options dictionary
    tab3 = con.read_parquet(test_file, options={}).to_arrow_table()
    assert tab3["geometry"].type.extension_name == "geoarrow.wkb"
    assert len(tab3) == len(tab1)  # Should be identical

    # Test 4: Options with string values
    tab4 = con.read_parquet(
        test_file, options={"test.option": "value"}
    ).to_arrow_table()
    assert tab4["geometry"].type.extension_name == "geoarrow.wkb"
    assert len(tab4) == len(
        tab1
    )  # Should be identical (option ignored but not errored)


def test_read_parquet_options_types(con, geoarrow_data):
    """Test that options parameter handles different Python types correctly"""
    test_file = geoarrow_data / "quadrangles/files/quadrangles_100k_geo.parquet"

    # Test with mixed data types
    options = {
        "string_option": "test_value",
        "bool_option": True,
        "int_option": 42,
        "float_option": 3.14,
        "none_option": None,
    }

    # This should not raise an error and should return valid data
    tab = con.read_parquet(test_file, options=options).to_arrow_table()
    assert tab["geometry"].type.extension_name == "geoarrow.wkb"
    assert len(tab) > 0


def test_read_parquet_options_multiple_files(con, geoarrow_data):
    """Test options parameter with multiple file input"""
    files = list(geoarrow_data.glob("example/files/*_geo.parquet"))

    # Test with multiple files and options
    tab1 = con.read_parquet(files).to_arrow_table()
    tab2 = con.read_parquet(files, options={"test": "value"}).to_arrow_table()

    # Results should be identical (options don't affect local files in this test)
    assert len(tab1) == len(tab2)
    assert tab1["geometry"].type.extension_name == "geoarrow.wkb"
    assert tab2["geometry"].type.extension_name == "geoarrow.wkb"


def test_read_parquet_options_remote_url(con):
    """Test options parameter with remote URL"""
    url = "https://raw.githubusercontent.com/geoarrow/geoarrow-data/v0.2.0/example/files/example_geometry_geo.parquet"

    # Test with remote URL and options
    tab1 = con.read_parquet(url).to_arrow_table()
    tab2 = con.read_parquet(url, options={"test.remote": "option"}).to_arrow_table()

    # Both should work and return same data
    assert len(tab1) == len(tab2)
    assert tab1["geometry"].type.extension_name == "geoarrow.wkb"
    assert tab2["geometry"].type.extension_name == "geoarrow.wkb"


@pytest.mark.parametrize(
    "options",
    [
        {"aws.region": "us-west-2"},
        {"aws.skip_signature": "true"},
        {"aws.nosign": "true"},
        {"aws.endpoint": "custom.endpoint.com"},
        {"aws.access_key_id": "test", "aws.secret_access_key": "test"},
    ],
)
def test_read_parquet_s3_options_interface(con, geoarrow_data, options):
    """Test that S3-related options are accepted without syntax errors"""
    # Use a local file to avoid actual S3 calls, but verify options are processed
    test_file = geoarrow_data / "quadrangles/files/quadrangles_100k_geo.parquet"

    # This should not raise a TypeError or similar interface error
    # (may raise other errors related to the file access, but not parameter errors)
    try:
        tab = con.read_parquet(test_file, options=options).to_arrow_table()
        assert tab["geometry"].type.extension_name == "geoarrow.wkb"
    except Exception as e:
        # If it fails, it should not be due to the options parameter interface
        assert "options" not in str(e).lower()
        assert "parameter" not in str(e).lower()
        assert "argument" not in str(e).lower()
