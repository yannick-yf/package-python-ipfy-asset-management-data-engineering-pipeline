"""Data Processing And Unification Process"""
import pandas as pd

from ipfy_asset_management_data_engineering_pipeline import (
    OrchestrationAndProcessExecutionPipeline
)

from ipfy_asset_management_pdf_to_table import (
    TextractZipToDataFrame
)

def test_orchestration_and_process_execution():

    # Read from csv to pandas dataframe the 2 exampels
    # To get this two examples we saved the output of textract_zip.get_csv_table_from_unzipped_file() 
    # from ipfy-asset-management-pdf-to-table package

    textract_zip = TextractZipToDataFrame(
    directory_to_extract_to = '.tests//data_example/unzipped'
    )

    list_file_unzipped = textract_zip.unzip_textract_zip_file(
        path_to_zip_file = './tests/data_example/example_BB_CC.zip'
    )

    test_dataframe_credit_agricole = textract_zip.get_csv_table_from_unzipped_file()

    DataPipeline = OrchestrationAndProcessExecutionPipeline(bank_records_data = test_dataframe_credit_agricole)
    bank_data_processed = DataPipeline.orchestration_and_process_execution()

    assert bank_data_processed.shape[0] > 0
    assert list(bank_data_processed.columns) == ['operation_date', 'operation_description', 'debit', 'credit']