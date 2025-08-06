import azure.functions as func
import logging
import json
import requests
import pyodbc
import os
import pandas as pd
import re
from datetime import datetime
from typing import Dict, List, Any, Optional

class DataValidator:
    """Class for validating and normalizing train data"""

    @staticmethod
    def validate_station_name(station: str) -> str:
        """Validate and normalize station name"""
        if not station or not isinstance(station, str):
            raise ValueError("Station name must be a non-empty string")

        # Normalize station name
        normalized = station.strip().title()

        # Basic validation - station names should be reasonable length
        if len(normalized) < 2 or len(normalized) > 100:
            raise ValueError(f"Invalid station name length: {normalized}")

        return normalized

    @staticmethod
    def validate_train_id(train_id: str) -> str:
        """Validate and normalize train ID"""
        if not train_id or not isinstance(train_id, str):
            return "Unknown"

        # Remove extra whitespace and normalize
        normalized = train_id.strip()

        # Train IDs should be alphanumeric with some special chars
        if not re.match(r"^[A-Za-z0-9\.\-\s]+$", normalized):
            logging.warning(f"Unusual train ID format: {normalized}")

        return normalized[:50]  # Truncate to fit database field

    @staticmethod
    def validate_timestamp(timestamp: Any) -> datetime:
        """Validate and convert timestamp to datetime"""
        if timestamp is None:
            raise ValueError("Timestamp cannot be None")

        try:
            # Handle string timestamps
            if isinstance(timestamp, str):
                timestamp_int = int(timestamp)
            else:
                timestamp_int = int(timestamp)

            # Reasonable timestamp range (not too far in past/future)
            if timestamp_int < 946684800:  # Year 2000
                raise ValueError("Timestamp too old")
            if timestamp_int > 4102444800:  # Year 2100
                raise ValueError("Timestamp too far in future")

            return datetime.fromtimestamp(timestamp_int)

        except (ValueError, OSError, OverflowError) as e:
            raise ValueError(f"Invalid timestamp: {timestamp} - {e}")

    @staticmethod
    def validate_delay(delay: Any) -> int:
        """Validate and normalize delay in minutes"""
        if delay is None or delay == "":
            return 0

        try:
            delay_seconds = int(delay)
            delay_minutes = delay_seconds // 60

            # Reasonable delay range (max 24 hours)
            if delay_minutes < -60 or delay_minutes > 1440:
                logging.warning(f"Unusual delay value: {delay_minutes} minutes")
                return min(max(delay_minutes, -60), 1440)  # Clamp to reasonable range

            return delay_minutes

        except (ValueError, TypeError):
            logging.warning(f"Invalid delay value: {delay}, defaulting to 0")
            return 0

    @staticmethod
    def validate_platform(platform: Any) -> str:
        """Validate and normalize platform"""
        if platform is None or platform == "":
            return "Unknown"

        platform_str = str(platform).strip()

        # Platform should be reasonable length
        if len(platform_str) > 20:
            platform_str = platform_str[:20]

        return platform_str
