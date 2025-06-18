from pathlib import Path

import pytest
import sedona_rs


HERE = Path(__file__).parent

CON = sedona_rs.connect()


@pytest.fixture()
def con():
    return CON


@pytest.fixture()
def geoarrow_data():
    return HERE.parent.parent.parent / "submodules" / "geoarrow-data"
