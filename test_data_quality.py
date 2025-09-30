"""
Test script to verify the improved wind data scraper works correctly.
Checks data quality and format consistency.
"""
import pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo
import logging

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

def test_scraped_data():
    """Test the quality and format of scraped data."""
    logger.info("=" * 70)
    logger.info("DATA QUALITY TEST")
    logger.info("=" * 70)
    
    # Get current date info
    tz = ZoneInfo("Europe/Rome")
    current_dt = datetime.now(tz)
    
    # Read the newly generated file
    file_path = f"data/measurements/measured_wind_venice_{current_dt.month}_{current_dt.year}.csv"
    
    try:
        df = pd.read_csv(file_path, parse_dates=['time_measured'])
        
        logger.info(f"\n📊 Dataset Overview:")
        logger.info(f"   File: {file_path}")
        logger.info(f"   Total Records: {len(df)}")
        logger.info(f"   Date Range: {df['time_measured'].min()} to {df['time_measured'].max()}")
        
        # Check for today's data
        today = current_dt.date()
        today_data = df[df['time_measured'].dt.date == today]
        
        logger.info(f"\n📅 Today's Data ({today}):")
        logger.info(f"   Records: {len(today_data)}")
        
        if len(today_data) > 0:
            logger.info(f"   ✅ Successfully scraped {len(today_data)} records for today")
            logger.info(f"\n   Latest 5 measurements:")
            latest = today_data.tail(5)
            for _, row in latest.iterrows():
                logger.info(f"   {row['time_measured']} | "
                          f"Speed: {row['wind_speed_measured']:.1f} | "
                          f"Gust: {row['wind_gust_measured']:.1f} | "
                          f"Dir: {row['wind_direction_measured']} ({row['wind_direction_degree_measured']}°)")
        else:
            logger.warning("   ⚠️  No data found for today")
        
        # Check column structure
        logger.info(f"\n🔍 Column Structure:")
        expected_columns = [
            'time_measured', 
            'wind_speed_measured', 
            'wind_gust_measured',
            'wind_direction_measured', 
            'wind_direction_degree_measured'
        ]
        
        for col in expected_columns:
            if col in df.columns:
                logger.info(f"   ✅ {col}: {df[col].dtype}")
            else:
                logger.error(f"   ❌ Missing column: {col}")
        
        # Check data types
        logger.info(f"\n🔍 Data Type Validation:")
        
        # Time column
        if pd.api.types.is_datetime64_any_dtype(df['time_measured']):
            logger.info("   ✅ time_measured: datetime64")
        else:
            logger.error("   ❌ time_measured: wrong type")
        
        # Numeric columns
        for col in ['wind_speed_measured', 'wind_gust_measured', 'wind_direction_degree_measured']:
            if pd.api.types.is_numeric_dtype(df[col]):
                logger.info(f"   ✅ {col}: numeric")
            else:
                logger.error(f"   ❌ {col}: not numeric")
        
        # String column
        if df['wind_direction_measured'].dtype == 'string' or df['wind_direction_measured'].dtype == 'object':
            logger.info("   ✅ wind_direction_measured: string/object")
        else:
            logger.warning(f"   ⚠️  wind_direction_measured: {df['wind_direction_measured'].dtype}")
        
        # Check for duplicates
        logger.info(f"\n🔍 Duplicate Check:")
        duplicates = df.duplicated(subset=['time_measured']).sum()
        if duplicates == 0:
            logger.info("   ✅ No duplicate timestamps found")
        else:
            logger.warning(f"   ⚠️  Found {duplicates} duplicate timestamps")
        
        # Check for missing values
        logger.info(f"\n🔍 Missing Value Check:")
        missing = df.isnull().sum()
        if missing.sum() == 0:
            logger.info("   ✅ No missing values")
        else:
            for col, count in missing[missing > 0].items():
                logger.warning(f"   ⚠️  {col}: {count} missing values")
        
        # Check time continuity (should be ~5 minute intervals)
        logger.info(f"\n🔍 Time Continuity Check:")
        df_sorted = df.sort_values('time_measured')
        time_diffs = df_sorted['time_measured'].diff()
        avg_interval = time_diffs.mean()
        logger.info(f"   Average interval: {avg_interval}")
        
        # Most common interval
        most_common_interval = time_diffs.mode()[0] if len(time_diffs.mode()) > 0 else None
        if most_common_interval:
            logger.info(f"   Most common interval: {most_common_interval}")
        
        # Statistics
        logger.info(f"\n📈 Wind Statistics:")
        logger.info(f"   Speed: min={df['wind_speed_measured'].min():.1f}, "
                   f"max={df['wind_speed_measured'].max():.1f}, "
                   f"avg={df['wind_speed_measured'].mean():.1f}")
        logger.info(f"   Gust: min={df['wind_gust_measured'].min():.1f}, "
                   f"max={df['wind_gust_measured'].max():.1f}, "
                   f"avg={df['wind_gust_measured'].mean():.1f}")
        
        # Check if sorted by time
        logger.info(f"\n🔍 Sort Order Check:")
        if df['time_measured'].is_monotonic_increasing:
            logger.info("   ✅ Data is sorted by timestamp (ascending)")
        else:
            logger.warning("   ⚠️  Data is not sorted by timestamp")
        
        logger.info("\n" + "=" * 70)
        logger.info("✅ DATA QUALITY TEST COMPLETE")
        logger.info("=" * 70)
        
        return True
        
    except Exception as e:
        logger.error(f"\n❌ Error during testing: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    test_scraped_data()
