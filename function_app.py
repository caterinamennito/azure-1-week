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
from data_validator import DataValidator


class TrainDataService:
    """Service class for handling train data operations"""

    def __init__(self):
        self.irail_base_url = "https://api.irail.be"
        self.connection_string = os.environ["SQL_CONNECTION_STRING"]

    def fetch_irail_liveboard_data(self, station: str) -> Dict[str, Any]:
        """
        Fetch live train departure data from iRail API

        Args:
            station: Station name to fetch data for

        Returns:
            Dict containing API response data
        """
        params = {"station": station, "format": "json", "lang": "en"}

        logging.info(f"Fetching data from iRail API for station: {station}")

        try:
            response = requests.get(
                f"{self.irail_base_url}/liveboard/", params=params, timeout=30
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logging.error(f"Failed to fetch data from iRail API: {e}")
            raise


class DatabaseManager:
    """Class for managing database operations"""

    def __init__(self, connection_string: str):
        self.connection_string = connection_string
        self._tables_created = False

    def _ensure_table_exists(self) -> None:
        """Create the required tables if they don't exist"""

        # Create train_departures table
        create_departures_table = """
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='train_departures' AND xtype='U')
        CREATE TABLE train_departures (
            id INT IDENTITY(1,1) PRIMARY KEY,
            station NVARCHAR(100) NOT NULL,
            train_id NVARCHAR(50) NOT NULL,
            destination NVARCHAR(100) NOT NULL,
            platform NVARCHAR(10),
            departure_time DATETIME NOT NULL,
            delay_minutes INT DEFAULT 0,
            canceled BIT DEFAULT 0,
            fetched_at DATETIME NOT NULL,
            INDEX IX_station_departure_time (station, departure_time)
        )
        """

        try:
            with pyodbc.connect(self.connection_string) as conn:
                cursor = conn.cursor()
                cursor.execute(create_departures_table)
                conn.commit()
                logging.info("Database tables ready")
                self._tables_created = True

        except pyodbc.Error as e:
            logging.error(f"Failed to create tables: {e}")
            raise

    def store_departures(self, departures: List[Dict[str, Any]], station: str) -> int:
        """
        Store train departure data with validation and normalization

        Args:
            departures: List of departure data from iRail API
            station: Station name

        Returns:
            Number of records stored
        """
        self._ensure_table_exists()

        if not departures:
            logging.info("No departures to store")
            return 0

        try:
            # Normalize station name
            normalized_station = DataValidator.validate_station_name(station)

            # Convert to DataFrame for easier data manipulation
            df = pd.DataFrame(departures)

            # Add station column
            df["station_name"] = normalized_station
            df["fetched_at"] = datetime.now()

            # Validate and normalize each record
            validated_records = []

            for _, row in df.iterrows():
                try:
                    validated_record = self._validate_departure_record(
                        row.to_dict(), normalized_station
                    )
                    validated_records.append(validated_record)
                except ValueError as e:
                    logging.warning(f"Skipping invalid departure record: {e}")
                    continue

            if not validated_records:
                logging.warning("No valid departure records to store")
                return 0

            # Convert back to DataFrame for bulk operations
            validated_df = pd.DataFrame(validated_records)

            # Remove duplicates based on key fields
            validated_df = validated_df.drop_duplicates(
                subset=["station", "train_id", "departure_time"], keep="last"
            )

            # Bulk insert
            stored_count = self._bulk_insert_departures(validated_df)

            logging.info(f"Stored {stored_count} validated departures")
            return stored_count

        except Exception as e:
            logging.error(f"Error in store_departures: {e}")
            raise

    def _extract_departure_data(self, departure: Dict[str, Any], station: str) -> tuple:
        """
        Extract and validate departure data

        Args:
            departure: Single departure data from API
            station: Station name

        Returns:
            Tuple of data ready for database insertion
        """
        train_id = departure.get("vehicle", "Unknown")
        destination = departure.get("station", "Unknown")
        platform = departure.get("platform", "Unknown")
        departure_time = departure.get("time", 0)
        delay = departure.get("delay", 0)

        # Convert timestamp to datetime
        try:
            departure_datetime = datetime.fromtimestamp(int(departure_time))
        except (ValueError, OSError) as e:
            raise ValueError(f"Invalid departure time: {departure_time}")

        # Convert delay from seconds to minutes
        delay_minutes = int(delay) // 60 if delay else 0

        return (
            station,
            train_id,
            destination,
            platform,
            departure_datetime,
            delay_minutes,
            datetime.now(),
        )

    def _validate_departure_record(
        self, record: Dict[str, Any], station: str
    ) -> Dict[str, Any]:
        """Validate and normalize a single departure record"""
        logging.info(f"Processing record: {record}")

        try:
            # Extract train ID - it's a direct string in the API response
            vehicle = record.get("vehicle", "Unknown")
            train_id = DataValidator.validate_train_id(vehicle)

            # Extract destination from stationinfo, not vehicle
            stationinfo = record.get("stationinfo", {})
            destination = (
                stationinfo.get("name", "Unknown")
                if isinstance(stationinfo, dict)
                else "Unknown"
            )

            if not destination or destination == "Unknown":
                raise ValueError("Missing destination")

            # Validate and normalize timestamp
            time_info = record.get("time", 0)
            departure_time = DataValidator.validate_timestamp(time_info)

            # Extract platform - it's a direct string in the API response
            platform = record.get("platform", "Unknown")
            platform = DataValidator.validate_platform(platform)

            # Validate delay
            delay_info = record.get("delay", 0)
            delay_minutes = DataValidator.validate_delay(delay_info)

            # Check for cancellation
            canceled = bool(int(record.get("canceled", 0)))

            return {
                "station": station,
                "train_id": train_id,
                "destination": destination,
                "platform": platform,
                "departure_time": departure_time,
                "delay_minutes": delay_minutes,
                "canceled": canceled,
                "fetched_at": datetime.now(),
            }

        except Exception as e:
            logging.error(f"Error validating record {record}: {e}")
            raise ValueError(f"Invalid departure record: {e}")

    def _bulk_insert_departures(self, df: pd.DataFrame) -> int:
        """Bulk insert departure records using pandas DataFrame"""
        insert_query = """
        INSERT INTO train_departures 
        (station, train_id, destination, platform, departure_time, delay_minutes, canceled, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """

        try:
            with pyodbc.connect(self.connection_string) as conn:
                cursor = conn.cursor()

                # Convert DataFrame to list of tuples for batch insert
                records = [
                    (
                        row["station"],
                        row["train_id"],
                        row["destination"],
                        row["platform"],
                        row["departure_time"],
                        row["delay_minutes"],
                        row.get("canceled", False),
                        row["fetched_at"],
                    )
                    for _, row in df.iterrows()
                ]

                cursor.executemany(insert_query, records)
                conn.commit()

                # Return the number of records we inserted instead of cursor.rowcount
                stored_count = len(records)
                logging.info(f"Bulk inserted {stored_count} records")

                return stored_count

        except pyodbc.Error as e:
            logging.error(f"Database error during bulk insert: {e}")
            raise


class TrainDataProcessor:
    """Main processor class that orchestrates the train data pipeline"""

    def __init__(self):
        self.train_service = TrainDataService()
        self.db_manager = DatabaseManager(self.train_service.connection_string)

    def process_station_data(self, station: str) -> Dict[str, Any]:
        """
        Process train data for a given station

        Args:
            station: Station name to process

        Returns:
            Dict containing processing results
        """
        try:
            # Fetch data from iRail API
            train_data = self.train_service.fetch_irail_liveboard_data(station)
            # Extract departures
            departures = train_data.get("departures", {}).get("departure", [])

            # Store in database
            stored_count = self.db_manager.store_departures(departures, station)

            return {
                "status": "success",
                "station": station,
                "trains_fetched": len(departures),
                "trains_stored": stored_count,
                "message": f"Successfully processed {stored_count} departures for {station}",
            }

        except Exception as e:
            logging.error(f"Error processing station {station}: {e}")
            return {"status": "error", "station": station, "message": str(e)}


# Create the function app
app = func.FunctionApp()


@app.function_name("fetch_train_data")
@app.route(route="trains", auth_level=func.AuthLevel.ANONYMOUS)
def fetch_train_data(req: func.HttpRequest) -> func.HttpResponse:
    """
    Azure Function to fetch train data from iRail API and store in SQL Database
    """
    logging.info("Python HTTP trigger function processed a request.")

    try:
        processor = TrainDataProcessor()

        # Get station parameter or use default
        station = req.params.get("station", "Brussels-Central")

        # Process the station data
        result = processor.process_station_data(station)

        # Return appropriate response
        status_code = 200 if result["status"] == "success" else 500

        return func.HttpResponse(
            json.dumps(result), status_code=status_code, mimetype="application/json"
        )

    except Exception as e:
        logging.error(f"Unexpected error in function: {e}")
        return func.HttpResponse(
            json.dumps({"status": "error", "message": "Internal server error"}),
            status_code=500,
            mimetype="application/json",
        )

@app.function_name("health_check")
@app.route(route="health", auth_level=func.AuthLevel.ANONYMOUS)
def health_check(req: func.HttpRequest) -> func.HttpResponse:
    """
    Azure Function to check the health of the service
    """
    logging.info("Health check function processed a request.")

    try:
        return func.HttpResponse(
            json.dumps({"status": "healthy"}),
            status_code=200,
            mimetype="application/json"
        )

    except Exception as e:
        logging.error(f"Unexpected error in health check function: {e}")
        return func.HttpResponse(
            json.dumps({"status": "error", "message": "Internal server error"}),
            status_code=500,
            mimetype="application/json",
        )

@app.function_name("timer_trigger")
@app.timer_trigger(schedule="0 */15 * * * *", arg_name="mytimer", run_on_startup=False)
def timer_trigger(mytimer: func.TimerRequest) -> None:
    """
    Timer trigger function to periodically fetch train data
    """
    logging.info("Timer trigger function started.")

    try:
        processor = TrainDataProcessor()
        # Main stations periodic updates
        stations = ["Brussels-Central", "Antwerp-Central", "Ghent-Saint-Peter's"]
        for station in stations:
            result = processor.process_station_data(station)
            logging.info(f"Processed {station}: {result['message']}")

    except Exception as e:
        logging.error(f"Error in timer trigger function: {e}")