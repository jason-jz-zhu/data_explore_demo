import pandas as pd
import operator
import numpy as np
import datetime

class quality_check:
    def __init__(self, df):
        """
        goal: init report dict.
        parameter df: Input Dataframe.
        """
        self.df =df
        self.report = {}

    def check_duplicate(self, cols=None):
        """
        goal: Check duplicate for dataframe based on columns.
        parameter cols: The list of columns name for checking dupliate. if None, that means
                        the list is ['As_of_Year', 'Agency_Code', 'Respondent_ID', 'Sequence_Number'].
        return: assign result to report
        """
        if cols is None:
            cols = ['As_of_Year', 'Agency_Code', 'Respondent_ID', 'Sequence_Number']
        if any(self.df[cols].duplicated()):
            tmp = self.df[self.df[cols].duplicated()][cols].drop_duplicates()
            self.report['duplication'.format(cols)] = tmp.reset_index(drop=True)
        else:
            self.report['duplication'.format(cols)] = None
    
    def null_count(self, col):
        """
        goal: Check null count for column
        parameter col: The column name
        return: Return the length of null
        """
        column_null = pd.isnull(col)
        null = col[column_null]
        return len(null)
    
    def check_missing_value(self, cols=None):
        """
        goal: Check missing values.
        parameter cols: The list of columns name for checking missing values. if None, that means
                        the list is all object type of columns.
        return: assign result to report
        """
        if cols is None:
            cols = self.df.select_dtypes(include=['object']).columns.tolist()
        # replace NA with np.nan
        loans_check_NA_res = {}
        for col in cols:
            NA_number = self.df[col].str.contains('NA').value_counts()
            if NA_number.index.contains(True):
                loans_check_NA_res[col] = NA_number[True]
            else:
                loans_check_NA_res[col] = 0
        loans_check_NA_res = sorted(loans_check_NA_res.items(), key = operator.itemgetter(1),reverse = True)
        for col in [key[0] for key in loans_check_NA_res if key[1] > 1]:
            self.df[col] = self.df[col].replace(r'^NA\s*', np.nan, regex=True)
        none_count = self.df.apply(self.null_count)
        res = none_count[none_count != 0].sort_values(ascending=False)
        self.report['missing value'] = np.round((res / len(self.df)) * 100, 2)
        #self.report['missing value'] = res / len(self.df)
    
    def convert_fill(self):
        # after deeply check each column, we decide the following columns which need to be converted to numeric.
        cols_to_numeric = ['Applicant_Income_000', 'Number_of_Owner_Occupied_Units', 'Tract_to_MSA_MD_Income_Pct',
                            'Census_Tract_Number', 'FFIEC_Median_Family_Income', 'Assets_000_Panel']
        for col in cols_to_numeric:
            self.df[col] = pd.to_numeric(self.df[col], errors="coerce")
        cols_fill_missing = ['Applicant_Income_000', 'Number_of_Owner_Occupied_Units', 'Tract_to_MSA_MD_Income_Pct',
                      'Census_Tract_Number', 'FFIEC_Median_Family_Income', 'Assets_000_Panel']
        self.df[cols_fill_missing] = self.df[cols_fill_missing].fillna(self.df[cols_fill_missing].mean())
    
    def outlier_calculate(self, col):
            c = 0.67448975019608171
            thresh = 3.5
            median = np.median(col)
            diff = np.abs(col - median)
            mad = np.median(diff)
            z_score = c * diff / mad
            return list(np.where(z_score > thresh)[0])
    
    def check_outlier(self, cols=None):
        """
        goal: Check outlier values.
        parameter cols: The list of columns name for checking outlier values. if None, that means
                        the list is all numeric type of columns.
        return: assign result to report
        """
        # convert numeric columns to numeric
        self.convert_fill()
        
        if cols is None:
            cols = self.df.select_dtypes(include=['int8', 'int64', 'float']).columns.tolist()
        total_rows = len(self.df)
        outlier_hashmap = {}
        for col in cols:
            tmp = self.outlier_calculate(self.df[col])
            outlier_hashmap[col] = np.round((len(tmp) / total_rows) * 100, 2)
        s = pd.Series(outlier_hashmap)
        self.report['outliers'] = s[s != 0].sort_values(ascending=False)
            
    def create_report(self):
        header_name = {
            'missing value': ['missing value proportion (%)'],
            'outliers': ['outliers proportion (%)']}
        file_name = 'data/quality check report {}.xlsx'.format(datetime.datetime.now().strftime("%Y%m%dT%H%M%S"))
        writer = pd.ExcelWriter(file_name)
        for key in self.report:
            if self.report[key] is not None:
                self.report[key].to_excel(writer, sheet_name=key, header=header_name[key])
        writer.save()
