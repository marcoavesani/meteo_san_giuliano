"""
Script to validate CSV format compatibility between old and new wind data files.
"""
import pandas as pd
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

def validate_csv_format(file1_path: str, file2_path: str) -> bool:
    """
    Compare two CSV files to ensure they have the same format.
    
    Args:
        file1_path: Path to first CSV file
        file2_path: Path to second CSV file
        
    Returns:
        True if formats match, False otherwise
    """
    logger.info("=" * 70)
    logger.info("CSV FORMAT VALIDATION")
    logger.info("=" * 70)
    
    try:
        # Read both files
        df1 = pd.read_csv(file1_path, parse_dates=['time_measured'])
        df2 = pd.read_csv(file2_path, parse_dates=['time_measured'])
        
        logger.info(f"\n📄 File 1: {Path(file1_path).name}")
        logger.info(f"   Rows: {len(df1)}")
        logger.info(f"   Columns: {list(df1.columns)}")
        
        logger.info(f"\n📄 File 2: {Path(file2_path).name}")
        logger.info(f"   Rows: {len(df2)}")
        logger.info(f"   Columns: {list(df2.columns)}")
        
        # Check 1: Column names match
        logger.info("\n🔍 Check 1: Column names...")
        if list(df1.columns) == list(df2.columns):
            logger.info("   ✅ Column names match perfectly")
        else:
            logger.error("   ❌ Column names don't match!")
            logger.error(f"   File 1: {list(df1.columns)}")
            logger.error(f"   File 2: {list(df2.columns)}")
            return False
        
        # Check 2: Column order
        logger.info("\n🔍 Check 2: Column order...")
        if df1.columns.tolist() == df2.columns.tolist():
            logger.info("   ✅ Column order is identical")
        else:
            logger.warning("   ⚠️  Column order differs (but names match)")
        
        # Check 3: Data types
        logger.info("\n🔍 Check 3: Data types...")
        dtypes_match = True
        for col in df1.columns:
            dtype1 = df1[col].dtype
            dtype2 = df2[col].dtype
            if dtype1 != dtype2:
                logger.warning(f"   ⚠️  Column '{col}': {dtype1} vs {dtype2}")
                dtypes_match = False
        
        if dtypes_match:
            logger.info("   ✅ All data types match")
        else:
            logger.info("   ⚠️  Some data type differences (may be acceptable)")
        
        # Check 4: Sample data comparison
        logger.info("\n🔍 Check 4: Sample data format...")
        logger.info(f"\n   First 3 rows of {Path(file1_path).name}:")
        logger.info(f"   {df1.head(3).to_string(index=False)}")
        
        logger.info(f"\n   First 3 rows of {Path(file2_path).name}:")
        logger.info(f"   {df2.head(3).to_string(index=False)}")
        
        # Check 5: Value ranges
        logger.info("\n🔍 Check 5: Value ranges...")
        for col in df1.select_dtypes(include=['number']).columns:
            logger.info(f"\n   Column: {col}")
            logger.info(f"   File 1 range: [{df1[col].min():.2f}, {df1[col].max():.2f}]")
            logger.info(f"   File 2 range: [{df2[col].min():.2f}, {df2[col].max():.2f}]")
        
        # Check 6: Missing values
        logger.info("\n🔍 Check 6: Missing values...")
        missing1 = df1.isnull().sum()
        missing2 = df2.isnull().sum()
        
        if missing1.sum() == 0 and missing2.sum() == 0:
            logger.info("   ✅ No missing values in either file")
        else:
            logger.info(f"\n   File 1 missing values:\n{missing1[missing1 > 0]}")
            logger.info(f"\n   File 2 missing values:\n{missing2[missing2 > 0]}")
        
        # Check 7: Time format
        logger.info("\n🔍 Check 7: Time format...")
        logger.info(f"   File 1 first timestamp: {df1['time_measured'].iloc[0]}")
        logger.info(f"   File 2 first timestamp: {df2['time_measured'].iloc[0]}")
        
        if pd.api.types.is_datetime64_any_dtype(df1['time_measured']) and \
           pd.api.types.is_datetime64_any_dtype(df2['time_measured']):
            logger.info("   ✅ Both files use datetime format for timestamps")
        else:
            logger.warning("   ⚠️  Timestamp format may differ")
        
        logger.info("\n" + "=" * 70)
        logger.info("✅ VALIDATION COMPLETE - Files are format compatible!")
        logger.info("=" * 70)
        return True
        
    except Exception as e:
        logger.error(f"\n❌ Error during validation: {e}")
        return False


if __name__ == "__main__":
    # Compare the newly generated file with an existing one
    new_file = "data/measurements/measured_wind_venice_9_2025.csv"
    old_file = "data/measurements/measured_wind_venice_7_2025.csv"
    
    validate_csv_format(old_file, new_file)
