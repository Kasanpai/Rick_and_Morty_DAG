"""Rick and Morty characters ETL DAG.

This DAG extracts character data from the Rick and Morty API,
transforms nested JSON fields to a flat table structure, and loads
records into the PostgreSQL staging layer.

Pipeline steps:
1. Extract characters from the external API.
2. Transform raw API records.
3. Load transformed records into PostgreSQL.
"""

from datetime import datetime
import time
from typing import Any

import pandas as pd
import requests
from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook


with DAG(
    dag_id="rick_and_morty_characters",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    doc_md=__doc__,
) as dag:

    @task
    def extract_characters() -> list[dict[str, Any]]:
        """Extract all characters from the Rick and Morty API.

        This task requests character data from the external API page by page.
        It handles retry logic, request timeouts, and small pauses between
        requests to reduce the risk of temporary API errors.

        Returns:
            list[dict[str, Any]]: A list of dictionaries with raw character
            data from the API.

        Raises:
            requests.exceptions.RequestException: If the API request fails
            after all retry attempts.
        """
        url: str | None = "https://rickandmortyapi.com/api/character"
        characters: list[dict[str, Any]] = []
        headers = {"User-Agent": "Mozilla/5.0"}

        while url:
            last_error: requests.exceptions.RequestException | None = None

            for _ in range(5):
                try:
                    response = requests.get(url, headers=headers, timeout=30)
                    response.raise_for_status()
                    break
                except requests.exceptions.RequestException as error:
                    last_error = error
                    time.sleep(5)
            else:
                raise last_error

            data = response.json()
            characters.extend(data["results"])
            url = data["info"]["next"]

            time.sleep(1)

        return characters

    @task
    def transform_characters(characters: list[dict[str, Any]],) -> list[dict[str, Any]]:
        """Transform raw character data to a flat table format.

        This task converts nested API fields into a flat structure suitable
        for loading into a relational database. It extracts origin and location
        names and URLs, converts the episode list into a comma-separated string,
        and removes nested columns from the final dataset.

        Args:
            characters (list[dict[str, Any]]): A list of dictionaries with raw
            character data from the API.

        Returns:
            list[dict[str, Any]]: A list of dictionaries with transformed
            character records.
        """
        df = pd.DataFrame(characters)

        def get_nested_value(value: dict[str, Any] | Any, key: str) -> Any:
            """Get value from nested dictionary."""
            if isinstance(value, dict):
                return value.get(key)
            return None

        df = df[
            [
                "id",
                "name",
                "status",
                "species",
                "type",
                "gender",
                "origin",
                "location",
                "image",
                "episode",
                "url",
                "created",
            ]
        ]

        df["origin_name"] = df["origin"].apply(
            lambda value: get_nested_value(value, "name")
        )
        df["origin_url"] = df["origin"].apply(
            lambda value: get_nested_value(value, "url")
        )
        df["location_name"] = df["location"].apply(
            lambda value: get_nested_value(value, "name")
        )
        df["location_url"] = df["location"].apply(
            lambda value: get_nested_value(value, "url")
        )
        df["episode"] = df["episode"].apply(
            lambda value: ",".join(value) if isinstance(value, list) else None
        )

        df = df.drop(columns=["origin", "location"])

        return df.to_dict(orient="records")

    @task
    def load_characters_to_postgres(records: list[dict[str, Any]]) -> None:
        """Load transformed character records into the PostgreSQL."""
        df = pd.DataFrame(records)

        hook = PostgresHook(postgres_conn_id="postgres_rick")
        engine = hook.get_sqlalchemy_engine()

        df.to_sql(
            name="characters",
            con=engine,
            schema="stg",
            if_exists="replace",
            index=False,
        )

    raw_characters = extract_characters()
    transformed_characters = transform_characters(raw_characters)
    load_characters_to_postgres(transformed_characters)
