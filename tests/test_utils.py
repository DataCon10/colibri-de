from scripts.utils import WindTurbineDataProcessor
import pytest


def test_read_data_success():
    test_data_paths = ["data/data_group_1.csv", "data/data_group_2.csv"]
    processor = WindTurbineDataProcessor(test_data_paths, "1 day")

    df = processor.read_data()

    assert not df.isEmpty()