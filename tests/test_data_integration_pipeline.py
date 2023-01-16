"""Data Processing And Unification Process"""
import pandas as pd

from ipfy_asset_management_data_engineering_pipeline import (
    DataProcessAndUnificationPipeline
)

from ipfy_asset_management_pdf_to_table import (
    TextractZipToDataFrame
)

def test_data_integration_pipeline():

    # Read from csv to pandas dataframe the 2 exampels
    # To get this two examples we saved the output of textract_zip.get_csv_table_from_unzipped_file() 
    # from ipfy-asset-management-pdf-to-table package

    textract_zip = TextractZipToDataFrame(
    directory_to_extract_to = '.tests//data_example/unzipped'
    )

    list_file_unzipped= textract_zip.unzip_textract_zip_file(
        path_to_zip_file = './tests/data_example/example_1.zip'
    )

    test_dataframe_credit_agricole = textract_zip.get_csv_table_from_unzipped_file()

    data_process_and_unification = DataProcessAndUnificationPipeline(bank_records_data = test_dataframe_credit_agricole)
    print(data_process_and_unification.orchestration_and_process_execution())

    assert test_dataframe_credit_agricole.shape[0] > 0