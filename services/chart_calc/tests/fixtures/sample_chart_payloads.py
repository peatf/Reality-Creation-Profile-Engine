# Sample payloads for testing chart calculation endpoints

# Standard case
payload_standard = {
    "birth_date": "1990-07-15",
    "birth_time": "10:30:00", # Assuming UTC
    "latitude": 34.0522,    # Los Angeles, CA
    "longitude": -118.2437
}

# Leap day case
payload_leap_day = {
    "birth_date": "1992-02-29",
    "birth_time": "01:15:00", # Assuming UTC
    "latitude": 51.5074,    # London, UK
    "longitude": 0.1278
}

# Southern Hemisphere case
payload_southern_hemisphere = {
    "birth_date": "1985-12-01",
    "birth_time": "23:55:00", # Assuming UTC
    "latitude": -33.8688,   # Sydney, Australia
    "longitude": 151.2093
}

# Extreme latitude (North)
payload_extreme_north = {
    "birth_date": "2000-06-21",
    "birth_time": "12:00:00", # Assuming UTC
    "latitude": 80.0000,    # Arctic region
    "longitude": 0.0000
}

# Extreme latitude (South) - Note: Skyfield might have limitations or specific behaviors
# with extreme polar regions, especially for house systems.
payload_extreme_south = {
    "birth_date": "2001-12-21",
    "birth_time": "00:00:00", # Assuming UTC
    "latitude": -80.0000,   # Antarctic region
    "longitude": 0.0000
}

# Case with birth time near midnight
payload_midnight_transition = {
    "birth_date": "1995-03-10",
    "birth_time": "00:01:30", # Assuming UTC, just after midnight
    "latitude": 40.7128,    # New York, NY
    "longitude": -74.0060
}

payload_before_midnight_transition = {
    "birth_date": "1995-03-09", # Day before
    "birth_time": "23:58:30", # Assuming UTC, just before midnight
    "latitude": 40.7128,    # New York, NY
    "longitude": -74.0060
}


# Invalid data for testing validation
payload_invalid_date = {
    "birth_date": "1990-13-01", # Invalid month
    "birth_time": "10:30:00",
    "latitude": 34.0522,
    "longitude": -118.2437
}

payload_invalid_time = {
    "birth_date": "1990-01-01",
    "birth_time": "25:30:00", # Invalid hour
    "latitude": 34.0522,
    "longitude": -118.2437
}

payload_invalid_latitude = {
    "birth_date": "1990-01-01",
    "birth_time": "10:30:00",
    "latitude": 91.0, # Invalid latitude
    "longitude": -118.2437
}

payload_missing_field = { # Example: missing birth_time
    "birth_date": "1990-01-01",
    "latitude": 34.0522,
    "longitude": -118.2437
}

# List of valid payloads for parameterized tests
valid_payloads = [
    payload_standard,
    payload_leap_day,
    payload_southern_hemisphere,
    payload_extreme_north,
    # payload_extreme_south, # May require special handling or mock for certain calcs
    payload_midnight_transition,
    payload_before_midnight_transition
]