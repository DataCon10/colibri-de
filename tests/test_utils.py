from scripts.utils import WindTurbineDataProcessor
import pytest


def test_read_data_success():
    test_data_paths = ["data/data_group_1.csv", "data/data_group_2.csv"]
    processor = WindTurbineDataProcessor(test_data_paths, "1 day")

    try:
        processor.read_data()
        assert True
    except:
        assert False, "read_data method failed with valid data paths"