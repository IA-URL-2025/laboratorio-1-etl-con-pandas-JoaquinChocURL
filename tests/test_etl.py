import pandas as pd
from src.etl import run_etl
import os

def test_output_file_exists():
    run_etl()
    assert os.path.exists("data/output.csv")

def test_output_content():
    run_etl()
    result = pd.read_csv("data/output.csv")
    expected = pd.read_csv("data/expected_output.csv")

    pd.testing.assert_frame_equal(
        result.reset_index(drop=True),
        expected.reset_index(drop=True)
    )
