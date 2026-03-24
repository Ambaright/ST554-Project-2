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
        Append boolean column indicating whether numeric values fall within bound
        '''