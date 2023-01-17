from typing import List
import pandas as pd

class BankDataTypeDetection:
    """
    Data Process to detect the bank type
    """

    # # init method or constructor
    # def __init__(self, bank_records_data):
    #     self.bank_records_data = bank_records_data


    def get_bank_file_type_from_strucutre(self, bank_records_data_columns) -> str:
        """
        From the dataframe strucutre we defined the function we want to run to unified the data
        """

        # List strucutre from different banks
        columns_credit_agricole = ["'Date ope. ", "'Date valeur ", "'Libellé des opérations ", "'Débit ", "'Crédit ", "'", "Unnamed: 6", "'Date opé. "]

        if list(bank_records_data_columns) == columns_credit_agricole:
            return 'CreditAgricoleAlsace'
        else:
            return 'DontKnowYet'