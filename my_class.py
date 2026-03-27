# Author: Amanda Baright
# Purpose: ST554 Project 2 Task 1
# Date 03.24.2026

# Import required packages
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from functools import reduce
from pyspark.sql.types import *
import pandas as pd

# Create SparkDataCheck Class
class SparkDataCheck:
    '''
    A data quality class for Spark SQL data frames
    '''
    
    def __init__(self, df: DataFrame):
        '''
        An __init__ function that takes in self and a dataframe argument
        Creates a .df attibute that is the dataframe
        '''
        self.df = df
    
    # Create a @classmethod that creates an instance while reading in a csv file
    @classmethod
    def from_csv(cls, spark, path):
        '''
        This method will create an instance while reading in a csv file.
        
        This method has arguments for the class, the spark session, and the csv path to the file.
        
        We will use the spark.read.load() function to create a Spark SQL DataFrame
        
        We then return an object of our class.
        '''
        df = spark.read.load(path,
                             format = "csv",
                             sep = ",",
                             inferSchema = "true",
                             ignoreLeadingWhiteSpace = "true",
                             ignoreTrailingWhiteSpace = "true",
                             header = "true")
        return cls(df)

        
    
    # Create a @classmethod for creating an instance from a pandas dataframe
    @classmethod
    def from_pandas(cls, spark, pandas_df):
        '''
        This method will create an instance from a pandas dataframe.
        
        This method has arguments for the class, the spark session, and the pandas dataframe.
        
        We will use the spark.createDataFrame() function to create a Spark SQL DataFrame.
        
        We then return an object of our class.
        '''
        df = spark.createDataFrame(pandas_df)
        return cls(df)
    
    
    #################################
    # Validation methods
    
    # Create Boolean column based on numeric bounds
    def check_numeric_range(self, column: str, lower: str = None, upper: str = None):
        '''
        Append a Boolean column indicating whether numeric values fall within bound.
        
        The function allows the users to supply a single column and a lower and upper bound value.
        The method will check that at least one lower or upper is provided.
        Any Null values are returned as Null.
        The method will also checks if the user supplied a non-numeric column.
        If a non-numeric column is provided, then a message is printed and the dataframe is returned without modification.
        
        The function returns the dataframe with an appended column of Boolean values.
        '''
        
        # Grab the data type of the column provided
        col_type = None
        for name, dtype in self.df.dtypes: # Unpacks the list tuple from .dtypes
            if name == column:
                col_type = dtype
                break
        
        numeric_types = ["float", "int", "longint", "bigint", "double", "integer"] # Allowed numeric types
        
        # Logic to check if col_type is in our numeric_types list
        if col_type is None or not any(type in col_type.lower() for type in numeric_types):
            print(f"Message: Column '{column}' is non-numeric.")
            return self # Return dataframe without modifications
        
        # Check that at least one of lower or upper is provided
        if lower is None and upper is None:
            return self
        
        # Check if the numeric column is within user defined bounds
        col_obj = F.col(f"`{column}`") # PySpark Column Object
        if lower is not None and upper is not None: # Check that both lower and upper provided
            condition = col_obj.between(lower, upper)
        elif lower is not None: # Check that lower was provided
            condition = col_obj >= lower
        else: # Check that upper was provided
            condition = col_obj <= upper
            
        # Create new column with the Boolean values
        self.df = self.df.withColumn(f"{column}_in_bounds", condition)
        return self
    
    # Create a Boolean column based on string levels
    def check_string_levels(self, column, levels):
        '''
        Append a Boolean column indicating whether a user supplied string column falls within a
        user specified set of levles. Return the dataframe with the appended columns of Boolean values.
        
        Null values, return Null.
        If the user supplies a non-string column, a message is printed and the df is returned without modification.
        '''
        
        # Grab the data type of the column provided
        col_type = None
        for name, dtype in self.df.dtypes: # Unpacks the list tuple from .dtypes
            if name == column:
                col_type = dtype
                break
                
        # Check if the column is a string
        if col_type != 'string':
            print(f"Message: Column '{column}' is not a string column.")
            return self
        
        # If the column is a string, append a Boolean column that determines if the string falls within the specified set of levels.
        # Also, ensure that Null values are returned Null
        self.df = self.df.withColumn(
            f"{column}_valid_level",
            F.when(F.col(f"`{column}`").isNull(), None)
                .otherwise(F.col(f"`{column}`").isin(levels))
        )
        return self
    
    # Create a Boolean column to check for missing values in a provided column
    def check_missing(self, column):
        '''
        Append a Boolean column indicating whether a user supplied column has missing (Null) values.
        '''
        # Check if each value in a column is Null
        self.df = self.df.withColumn(
            f"{column}_is_null",
            F.col(f"`{column}`").isNull()
        )
        return self
    
    ####################################
    # Summarization Methods
    # Each summarization method will return the summarization of the data (not an ammended dataframe) as a pandas data frame
    
    # Min and Max Summarization Method
    def report_min_max(self, column = None, group_var = None):
        '''
        A summarization method to report the min and max of a numeric column supplied by the user.
        This method will check if the provided column is numeric. If not, a message will be printed and None returned.
        If the column is numeric, the min and max of the column will be reported.
        The user also has the option to add a grouping variable, in which the data will be grouped appropriately and the
        min and max will be reported.
        If no column is supplied, the method will report the min and max of any numeric column, and grouped appropriately.
        '''
        
        numeric_types = ["float", "int", "longint", "bigint", "double", "integer"] # Allowed numeric types
        
        if column:
            # Grab the data type of the column provided
            col_type = None
            for name, dtype in self.df.dtypes: # Unpacks the list tuple from .dtypes
                if name == column:
                    col_type = dtype
                    break
            
            # Logic to check if col_type is in our numeric_types list
            if col_type is None or not any(type in col_type.lower() for type in numeric_types):
                print(f"Message: Column '{column}' is non-numeric.")
                return None
            target_col = [column]
            
        else:
            #Finds all numeric columns using the unpacking logic
            target_col = [col for col, type in self.df.dtypes if any(num_type in type.lower() for num_type in numeric_types)]
            
        # List to store individual panda data frames
        pdf_list = []
        
        for col in target_col:
            # Create a small summary for one column
            working_df = self.df.groupBy(group_var) if group_var else self.df
            
            # Perform min and max and convert to Pandas
            col_summary = working_df.agg(
                F.min(f"`{col}`").alias(f"{col}_min"),
                F.max(f"`{col}`").alias(f"{col}_max")
            ).toPandas()
            
            # Append col_summary to pdf list
            pdf_list.append(col_summary)
            
        # Use reduce() from functools to merge all dataframes in the list together
        # If group_var exists, we merge on that column, otherwise its a simple join
        if group_var:
            final_pdf = reduce(lambda left, right: pd.merge(left, right, on = group_var), pdf_list)
        else:
            # If no grouping, then side-by-side with axis = 1
            final_pdf = pd.concat(pdf_list, axis = 1)
            
        # Return final pdf
        return final_pdf
    
    
    # Counting Summarization Method
    def report_counts(self, col1, col2 = None):
        '''
        A summarization method to report the counts associated with one or two string columns.
        The function requires one column to be provided, but users have the option to include a second argument for a second string column.
        The function will check that the columns are string columns. 
        If so, they will report the counts for the combinations of levels of each variable or the single variable.
        If not, then a message will be printed if the column is numeric.
        '''
        
        # Create a list of the provided columns
        columns = [col1] if col2 is None else [col1, col2] 
        allowed_types = ['string', 'boolean']
        
        for col in columns:
            # Grab the data type of the column provided
            col_type = None
            for name, dtype in self.df.dtypes: # Unpacks the list tuple from .dtypes
                if name == col:
                    col_type = dtype
                    break
                    
            # Check if col_type is a string
            if col_type not in allowed_types:
                print(f"Message: Column '{col}' is numeric/non-string.")
                return None
            
        # Return the counts of each provided column as a panda data frame
        safe_cols = [f"`{col}`" for col in columns]
        return self.df.groupBy(*safe_cols).count().toPandas()