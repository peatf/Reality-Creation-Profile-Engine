from timezonefinder import TimezoneFinder
import pytz
from datetime import datetime, date, time, timezone
from pydantic import BaseModel, Field
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize TimezoneFinder. The instance creation can be time-consuming,
# so it's better to do it once when the module is loaded.
try:
    tf = TimezoneFinder()
    logger.info("TimezoneFinder initialized successfully.")
except Exception as e:
    logger.error(f"Failed to initialize TimezoneFinder: {e}")
    tf = None

class TimezoneError(Exception):
    """Custom exception for timezone lookup errors."""
    pass

class HistoricalTimezoneInfo(BaseModel):
    """Model for storing historical timezone results."""
    iana_timezone: str = Field(..., description="The IANA timezone identifier (e.g., 'America/New_York')")
    localized_datetime: datetime = Field(..., description="The birth datetime localized to the historical timezone, including the correct offset")
    utc_offset_seconds: int = Field(..., description="The UTC offset in seconds at the specific birth datetime")

    @property
    def utc_offset_str(self) -> str:
        """Returns the UTC offset as a string (e.g., '-07:00')."""
        offset = self.localized_datetime.utcoffset()
        if offset is None:
            return "+00:00" # Should not happen for localized datetime
        total_seconds = int(offset.total_seconds())
        hours, remainder = divmod(abs(total_seconds), 3600)
        minutes, _ = divmod(remainder, 60)
        sign = "-" if total_seconds < 0 else "+"
        return f"{sign}{hours:02d}:{minutes:02d}"


def get_historical_timezone_info(latitude: float, longitude: float, birth_date: date, birth_time: time) -> HistoricalTimezoneInfo:
    """
    Determines the historical IANA timezone and UTC offset for a specific location and datetime.

    Args:
        latitude: Latitude of the birth location.
        longitude: Longitude of the birth location.
        birth_date: The date of birth.
        birth_time: The time of birth.

    Returns:
        A HistoricalTimezoneInfo object containing the IANA timezone name and
        the localized birth datetime with the correct historical offset.

    Raises:
        TimezoneError: If TimezoneFinder is not initialized, timezone cannot be found,
                       or localization fails.
    """
    if not tf:
        raise TimezoneError("TimezoneFinder not initialized.")

    # Combine date and time into a naive datetime object
    naive_dt = datetime.combine(birth_date, birth_time)

    # Find the timezone name at the given coordinates
    logger.info(f"Looking up timezone for Lat={latitude}, Lng={longitude}")
    timezone_str = tf.timezone_at(lng=longitude, lat=latitude)

    if timezone_str is None:
        logger.warning(f"Could not find timezone for Lat={latitude}, Lng={longitude}")
        # Fallback or error handling strategy:
        # Option 1: Raise an error
        raise TimezoneError(f"Could not determine timezone for coordinates ({latitude}, {longitude}).")
        # Option 2: Return a default (e.g., UTC) - Less accurate
        # timezone_str = "UTC"
        # logger.warning("Falling back to UTC timezone.")

    logger.info(f"Found timezone: {timezone_str}")

    try:
        # Get the pytz timezone object
        tz = pytz.timezone(timezone_str)
    except pytz.UnknownTimeZoneError:
        logger.error(f"Unknown timezone identifier returned by timezonefinder: {timezone_str}")
        raise TimezoneError(f"Invalid timezone identifier: {timezone_str}")

    try:
        # Localize the naive datetime. This is the crucial step where pytz applies
        # the correct historical offset, including DST rules for that *specific* moment.
        # is_dst=None allows pytz to determine DST automatically, raising AmbiguousTimeError
        # or NonExistentTimeError if the time is ambiguous or invalid during DST transitions.
        localized_dt = tz.localize(naive_dt, is_dst=None)
        logger.info(f"Localized datetime: {localized_dt} with offset {localized_dt.utcoffset()}")

    except pytz.AmbiguousTimeError:
        # Handle times that occurred twice during DST fallback ("fall back")
        # Typically, the first occurrence (standard time) is assumed unless specified otherwise.
        # You might need user clarification or a specific rule here.
        logger.warning(f"Ambiguous time encountered for {naive_dt} in {timezone_str}. Assuming first occurrence (standard time).")
        localized_dt = tz.localize(naive_dt, is_dst=False) # Or is_dst=True for the second occurrence (DST)
    except pytz.NonExistentTimeError:
        # Handle times that did not exist during DST spring forward ("spring forward")
        logger.error(f"Non-existent time encountered for {naive_dt} in {timezone_str}.")
        raise TimezoneError(f"The time {birth_time} did not exist on {birth_date} in {timezone_str} due to DST transition.")
    except Exception as e:
        logger.error(f"An unexpected error occurred during timezone localization: {e}")
        raise TimezoneError(f"Unexpected error during localization: {e}")

    offset_seconds = int(localized_dt.utcoffset().total_seconds())

    return HistoricalTimezoneInfo(
        iana_timezone=timezone_str,
        localized_datetime=localized_dt,
        utc_offset_seconds=offset_seconds
    )

# Example usage (for testing purposes)
if __name__ == '__main__':
    # Example 1: New York before DST change in 2007
    try:
        # March 10, 2007 - Standard Time (UTC-5)
        info1 = get_historical_timezone_info(40.7128, -74.0060, date(2007, 3, 10), time(10, 0, 0))
        print(f"NYC 2007-03-10 10:00: {info1.iana_timezone}, Offset: {info1.utc_offset_str}, Localized: {info1.localized_datetime}")

        # March 11, 2007 02:30 - Non-existent time due to DST spring forward (shift happened at 2 AM)
        try:
            get_historical_timezone_info(40.7128, -74.0060, date(2007, 3, 11), time(2, 30, 0))
        except TimezoneError as e:
            print(f"NYC 2007-03-11 02:30: Expected Error -> {e}")

        # March 11, 2007 03:30 - Daylight Time (UTC-4)
        info3 = get_historical_timezone_info(40.7128, -74.0060, date(2007, 3, 11), time(3, 30, 0))
        print(f"NYC 2007-03-11 03:30: {info3.iana_timezone}, Offset: {info3.utc_offset_str}, Localized: {info3.localized_datetime}")

        # Example 2: London during BST
        info_london = get_historical_timezone_info(51.5074, -0.1278, date(2023, 7, 15), time(12, 0, 0))
        print(f"London 2023-07-15 12:00: {info_london.iana_timezone}, Offset: {info_london.utc_offset_str}, Localized: {info_london.localized_datetime}")

        # Example 3: Sydney during AEST (Southern Hemisphere winter)
        info_sydney = get_historical_timezone_info(-33.8688, 151.2093, date(2023, 7, 15), time(12, 0, 0))
        print(f"Sydney 2023-07-15 12:00: {info_sydney.iana_timezone}, Offset: {info_sydney.utc_offset_str}, Localized: {info_sydney.localized_datetime}")

    except TimezoneError as e:
        print(f"Error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")