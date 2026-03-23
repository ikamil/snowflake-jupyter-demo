"""
Sample Luigi ETL Task
Demonstrates: Luigi task with PySpark, egg distribution,
  idempotency via complete() check, parameterized execution
"""
import os
import luigi
from pyspark.sql import SparkSession


class BuildEgg(luigi.Task):
    """Build a distributable egg for Spark workers."""

    def output(self):
        return luigi.LocalTarget('dist/sample_etl_pipeline-1.0.egg')

    def run(self):
        os.system('python setup.py bdist_egg > /dev/null')


class RunPipeline(luigi.Task):
    """
    Main ETL task: reads from distributed filesystem,
    transforms with Spark, writes results to warehouse.
    """
    run_date = luigi.DateParameter()
    input_path = luigi.Parameter(default='/data/raw/observations')
    output_path = luigi.Parameter(default='/data/processed/aggregated')

    def requires(self):
        return BuildEgg()

    def complete(self):
        """Idempotency: check if output partition already exists."""
        target = os.path.join(
            self.output_path,
            f'dt={self.run_date.isoformat()}',
            '_SUCCESS'
        )
        return os.path.exists(target)

    def run(self):
        spark = SparkSession.builder \
            .appName(f'etl-pipeline-{self.run_date}') \
            .config('spark.executor.memory', '10g') \
            .config('spark.executor.cores', '4') \
            .config('spark.sql.shuffle.partitions', '2000') \
            .config('spark.default.parallelism', '260') \
            .getOrCreate()

        # Read raw observations
        df = spark.read.parquet(
            os.path.join(self.input_path, f'dt={self.run_date.isoformat()}')
        )

        # Register custom UDFs from JARs
        spark.sql("ADD JAR /opt/jars/location_api.jar")
        spark.sql(
            "CREATE TEMPORARY FUNCTION resolve_grid "
            "AS 'com.example.udf.ResolveGrid'"
        )

        # Transform: aggregate by grid cell, compute metrics
        df.createOrReplaceTempView('raw_observations')
        result = spark.sql("""
            SELECT
                grid_cell_id,
                COUNT(DISTINCT device_id)  AS unique_devices,
                COUNT(*)                   AS total_signals,
                MIN(observation_ts)        AS first_seen,
                MAX(observation_ts)        AS last_seen
            FROM raw_observations
            GROUP BY grid_cell_id
        """)

        # Write partitioned output
        result.write \
            .mode('overwrite') \
            .partitionBy('grid_cell_id') \
            .parquet(
                os.path.join(self.output_path, f'dt={self.run_date.isoformat()}')
            )

        spark.stop()


if __name__ == '__main__':
    luigi.run()
