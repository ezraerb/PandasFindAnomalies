#!/usr/bin/python
""" 
In the Adventureworks data set, find days where number of sales are unusually high or
unusually low by InterQuantile Range analysis.
This data set is one of the classic sets for demonstrating ETL and analysis code. It is 
publicly available at https://github.com/lorint/AdventureWorks-for-Postgres
"""

from configparser import ConfigParser
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from sqlalchemy.sql import text


def get_config(filename: str = "database.ini") -> dict:
    """Read access credentials for Postgres"""
    config = "postgresql"

    parser = ConfigParser()
    parser.read(filename)

    db_config = {}
    if parser.has_section(config):
        params = parser.items(config)
        for param in params:
            db_config[param[0]] = param[1]
    else:
        raise ValueError(f"Section postgresql not found in {filename}")
    return db_config


def connect():
    """Connect to the PostgreSQL database server"""
    try:
        params = get_config()
        # Pandas recommends using a SQLAlchemy engine for the connrection,
        # requiring the following
        url = URL.create(drivername="postgresql", **params)
        return create_engine(url)

    except Exception as error:  # pylint: disable=broad-exception-caught
        print(error)
        return None


def days_unusual_sale_counts(engine):
    """Find days with statistically significant unusual numbers of sales"""

    # Fetch the count of transactions by date, ignoring the time components. Done in SQL
    # to limit the amount of data fetched
    with engine.connect() as conn:
        sql = text(
            "select orderdate::date, count (*) from sales.salesorderheader group by orderdate::date"
        )
        transaction_counts = pd.read_sql(sql, conn)

        # The Inter Quatrile Range of outlier detection works as follows:
        # Find the 1st and 3rd Quadrile values, and the difference between them. This gives the
        # range of the middle half of the dataset
        quantiles = transaction_counts["count"].quantile([0.25, 0.75])
        q_1 = quantiles[0.25]
        q_3 = quantiles[0.75]
        iqr = q_3 - q_1

        # Anything below the first quadrile - 1.5*IQR, or above third quadrile + 1.5*IQR, is
        # considered an outlier. The acceptable values covers 2.7 standard deviations in each
        # direction assuming a Gausian distribution.
        lower_bound = q_1 - (1.5 * iqr)
        upper_bound = q_3 + (1.5 * iqr)

        anomalies = transaction_counts[
            ~transaction_counts["count"].between(lower_bound, upper_bound)
        ].sort_values(by=["orderdate"])
        anomalies.to_csv("dates_unusual_sales.csv", index=False)


if __name__ == "__main__":
    db_engine = connect()
    if db_engine:
        days_unusual_sale_counts(db_engine)
