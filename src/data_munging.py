import pandas as pd
import datetime

data_files = [
    {
        'file': '2012_to_2014_institutions_data.csv',
        'type': {
            'As_of_Year': 'int64',
            'Agency_Code': 'str',
            'Respondent_ID': 'str',
            'Respondent_Name_TS': 'str',
            'Respondent_City_TS': 'str',
            'Respondent_State_TS': 'str',
            'Respondent_ZIP_Code': 'str',
            'Parent_Name_TS': 'str',
            'Parent_City_TS': 'str',
            'Parent_State_TS': 'str',
            'Parent_ZIP_Code': 'str',
            'Assets_000_Panel': 'int64'
        }
    },
    {
        'file': '2012_to_2014_loans_data.csv',
        'type': {
            'As_of_Year': 'int64',
            'Agency_Code': 'str',
            'Agency_Code_Description': 'str',
            'Respondent_ID': 'str',
            'Sequence_Number': 'int64',
            'Loan_Amount_000': 'int64',
            'Applicant_Income_000': 'str',
            'Loan_Purpose_Description': 'str',
            'Loan_Type_Description': 'str',
            'Lien_Status_Description': 'str',
            'State': 'str',
            'State_Code': 'int8',
            'County_Name': 'str',
            'County_Code': 'str',
            'MSA_MD': 'str',
            'MSA_MD_Description': 'str',
            'Census_Tract_Number': 'str',
            'FFIEC_Median_Family_Income': 'str',
            'Tract_to_MSA_MD_Income_Pct': 'str',
            'Number_of_Owner_Occupied_Units': 'str',
            'Conforming_Limit_000': 'float64',
            'Conventional_Status': 'str',
            'Conforming_Status': 'str',
            'Conventional_Conforming_Flag': 'str'
        }
    }
]


class data_munging:
    def __init__(self, path='data/'):
        """
        goal: read two datasets and store them into a dict.
        """
        self.data = {}
        for file in data_files:
            f_name, f_type = file['file'], file['type']
            df = pd.read_csv('{}{}'.format(path, f_name), dtype=f_type)
            key = f_name.replace('.csv', '').split('_')[-2]
            self.data[key] = df

    def loan_category(self, loan):
        """
        goal: Category Loan_Amount_000 into six groups
                based on data distribution 150 is the point 25%,
                235 is the point 50%, 550 is the point 75%
        parameter loan: Series
        return: category
        """
        if loan <= 150:
            return 'low'
        elif 150 <= loan < 235:
            return 'medium'
        elif 235 <= loan < 347:
            return 'high'
        elif 347 <= loan < 550:
            return 'very high'
        elif 550 <= loan < 1000:
            return 'extremely high'
        else:
            return 'unbelievable'
        
    def loan_merge_category(self, loan):
        """
        goal: Category Loan_Amount_000 into four groups
        parameter loan: Series
        return: category
        """
        if loan < 347:
            return 'normal'
        elif 347 <= loan < 550:
            return 'very high'
        elif 550 <= loan < 1000:
            return 'extremely high'
        else:
            return 'unbelievable'
        
    def hmda_init(self, full_size=None):
        """
        goal: merge loans and institutions data, and category Loan_Amount_000 into different groups
        parameter full_size: None or Yes
                             None includes all loans columns, Respondent_Name_TS, and two new categories
                             Yes includes all loans columns, all institutions columns, and two new categories
        return: Dataframe
        """
        try:
            if not full_size:
                institutions = self.data['institutions'][['As_of_Year', 'Respondent_ID', 'Agency_Code', 'Respondent_Name_TS']]
                size = 'Main part'
            else:
                institutions = self.data['institutions']
                size = 'Full size'
            combined = self.data['loans']
            combined = combined.merge(institutions, on=['As_of_Year', 'Respondent_ID', 'Agency_Code'], how='left')
            combined['Loans_Category'] = combined['Loan_Amount_000'].apply(self.loan_category)
            combined['Loan_Merge_Category'] = combined['Loan_Amount_000'].apply(self.loan_merge_category)
            print('{} datasets have been merged successfully. {} columns and {} rows'.format(size, len(combined.columns), len(combined)))
            return combined
        except:
            print('Merge data has error! Please check.')
            

    def hmda_to_json(self, df, states=None, conventional_conforming=None, path='data/'):
        """
        goal: Store JSON file based on filters (states and conventional_conforming).
        parameter df: An input dataframe.
        parameter states: A list of state name within 'DC', 'DE', 'MD', 'VA', 'WV'. if None, that means all states.
        parameter conventional_conforming: 'Y', 'N', or None. if None, that means no conventional_conforming filter.
        parameter path: The path of JSON file which will be stored.
        return: A JSON file based on filters. 
        """
        try:
            cache_df = df
            if states is not None and conventional_conforming is not None:
                cache_df = cache_df[(cache_df['State'].isin(states)) & (cache_df['Conventional_Conforming_Flag']==conventional_conforming)]
            elif states is not None and conventional_conforming is None:
                cache_df = cache_df[cache_df['State'].isin(states)]
            elif states is None and conventional_conforming is not None:
                cache_df = cache_df[cache_df['Conventional_Conforming_Flag']==conventional_conforming]
            file__location = '{}record_{}.json'.format(path, datetime.datetime.now().strftime("%Y%m%dT%H%M%S"))
            cache_df.to_json(file__location, orient='split')
            print('Dataset has been download at {} using states: {} and conventional_conforming: {}'.format(file__location, states, conventional_conforming))
        except:
            print('Convert JSON has error! Please check.')

