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
    
    #create boolean column based on numeric bounds
    def check_numeric_range(self, column: str, lower: str = None, upper: str = None):
        '''
        Append boolean column indicating whether numeric values fall within bound.
        
        The function allows the users to supply a single column and a lower and upper bound value.
        The method will check that at least one lower or upper is provided.
        Any Null values are returned as Null.
        The method will also checks if the user supplied a non-numeric column.
        If a non-numeric column is provided, then a message is printed and the dataframe is returned without modification.
        
        The function returns the dataframe with an appended column of Boolean values.
        '''
        
        # Check if the column is numeric.
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
        col_obj = F.col(column) # PySpark Column Object
        if lower is not None and upper is not None: # Check that both lower and upper provided
            condition = col_obj.between(lower, upper)
        elif lower is not None: # Check that lower was provided
            condition = col_obj >= lower
        else: # Check that upper was provided
            condition = col_obj <= upper
            
        # Create new column with the Boolean values
        self.df = self.df.withColumn(f"{column}_in_bounds", condition)
        return self