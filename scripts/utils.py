from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


class WindTurbineDataProcessor:
    """
        A class to process wind turbine data, including preprocessing, computing statistics,
        identifying and imputing outliers, and calculating aggregate metrics.

        Attributes:
            spark (SparkSession): SparkSession object.
            data_paths (list of str): List of paths to the data files.
            time_window (str): The time window for calculating statistics and aggregates, default is "1 day".
            df (DataFrame): The initial DataFrame loaded from data paths.
            df_preprocessed (DataFrame): The DataFrame after preprocessing, including standardisation of null values and timestamp processing.

        Methods:
            read_data(): Reads data from provided paths into a DataFrame.
            preprocess_data(): Standardises null values and processes timestamps in the DataFrame.
            standardise_null_values(): Replaces '' and '0' with null in the 'power_output' column and filters out these null values.
            process_timestamps(): Converts the 'timestamp' column to the correct timestamp format.
            compute_statistics(): Calculates standard deviation and mean of 'power_output' within each window.
            identify_and_impute_outliers(): Identifies outliers and imputes them with the mean 'power_output'.
            calculate_aggregates(): Calculates min, max, and average of 'power_output' within each window.
        """
    def __init__(self, data_paths, db_config, time_window="1 day"):
        """
        Constructs all the necessary attributes for the WindTurbineDataProcessor object.

        Parameters:
            data_paths (list of str): List of paths to the data files.
            time_window (str): The time window for calculating statistics and aggregates. Default is "1 day".
        """
        self.spark = SparkSession.builder.appName("Wind Turbine Data Processing").getOrCreate()
        self.data_paths = data_paths
        self.time_window = time_window
        self.df = self.read_data()
        self.df_preprocessed = None
        self.db_config = db_config or {}

    def read_data(self):
        """Reads data from provided paths into a DataFrame."""
        return self.spark.read.csv(self.data_paths, header=True, inferSchema=True)

    def preprocess_data(self):
        """Standardises null values and processes timestamps in the DataFrame."""
        self.standardise_null_values()
        self.process_timestamps()

    def standardise_null_values(self):
        """
        Replaces '' and '0' with None in the 'power_output' column of the DataFrame and filters out these None values.
        Assumes NULLs, 0s or blank values may exist due to system malfunctions. Filter out nulls first before calculating
        averages and then later impute the NULLs based on the averages.
        """
        self.df = self.df.withColumn("power_output",
                                     F.when(F.col("power_output").isin('', '0'), None)
                                     .otherwise(F.col("power_output")))
        self.df_preprocessed = self.df.filter(F.col("power_output").isNotNull())

    def process_timestamps(self):
        """
        Converts the 'timestamp' column values into the correct timestamp format 'dd/MM/yyyy HH:mm:ss'.
        """
        self.df_preprocessed = self.df_preprocessed.withColumn("timestamp",
                                                               F.to_timestamp(F.col("timestamp"),
                                                                              "dd/MM/yyyy HH:mm:ss"))

    def compute_statistics(self):
        """
        Calculates the standard deviation and mean of 'power_output' for each turbine within specified time windows.
        """
        window_spec = Window.partitionBy("turbine_id", F.window("timestamp", self.time_window))
        self.df_preprocessed = self.df_preprocessed.withColumn("power_output_stdev",
                                                               F.stddev("power_output").over(window_spec)) \
            .withColumn("power_output_mean",
                        F.mean("power_output").over(window_spec))

    def identify_and_impute_outliers(self):
        """
        Identifies outliers in 'power_output' as those beyond 2 standard deviations from the mean and imputes them with the mean value.
        """
        self.df_preprocessed = self.df_preprocessed.withColumn("is_outlier",
                                                               F.when(F.abs(F.col("power_output") - F.col(
                                                                   "power_output_mean")) > (
                                                                                  2 * F.col("power_output_stdev")), 1)
                                                               .otherwise(0))
        self.df_preprocessed = self.df_preprocessed.withColumn("power_output_imputed",
                                                               F.when(F.col("is_outlier") == 1,
                                                                      F.col("power_output_mean"))
                                                               .otherwise(F.col("power_output")))

    def calculate_aggregates(self):
        """
        Calculates the minimum, maximum, and average of 'power_output' for each turbine within specified time windows.
        """
        window_spec = Window.partitionBy("turbine_id", F.window("timestamp", self.time_window))
        self.df_preprocessed = self.df_preprocessed.withColumn("min_power_output",
                                                               F.min("power_output").over(window_spec)) \
            .withColumn("max_power_output", F.max("power_output").over(window_spec)) \
            .withColumn("avg_power_output", F.avg("power_output").over(window_spec))

    def write_to_database(self):
        """
        Writes the processed DataFrame to a database table as specified in the db_config.
        """
        if not self.db_config.get('jdbc_url') or not self.db_config.get('table_name') or not self.db_config.get(
                'properties'):
            raise ValueError("Database configuration is incomplete.")
        self.df_preprocessed.write.jdbc(url=self.db_config['jdbc_url'],
                                        table=self.db_config['table_name'],
                                        mode="overwrite",
                                        properties=self.db_config['properties'])

    # # Assumes NULLs, 0s or blank values may exist due to system malfunctions. Filter out nulls first before calculating
    # # averages and then later impute the NULLs based on the averages

    # def remove_null_values(self):
    #     self.df = self.df.withColumn("power_output",
    #                                  F.when(F.col("power_output").isin('', '0'), None)
    #                                  .otherwise(F.col("power_output")))
    #     self.df_preprocessed = self.df.filter(F.col("power_output").isNotNull())


    # def filter_nulls(self):
    #     df_standardised_for_nulls = self.df.withColumn("power_output",
    #                                                   F.when(F.col("power_output").isin('', '0'), None)
    #                                                   .otherwise(F.col("power_output")))
    #
    #     self.df_filtered = df_standardised_for_nulls.filter(self.df.power_output.isNotNull())
    #
    #     self.df_filtered_out_nulls = self.df_filtered.filter(self.df.power_output.isNotNull())
    #
    # # Ensures timestamps are in the correct format
    # def process_ts(self):
    #
    #     self.df_preprocessed = self.df_filtered_out_nulls.withColumn("timestamp", F.to_timestamp(F.col("timestamp"),
    #                                                                                              "dd/MM/yyyy HH:mm:ss"))
    #
    # # Computes stdev and mean after NULLs have been removed, ensuring results aren't skewed
    # def compute_stats(self, time_window):
    #     # Define the window specification e.g. "1 day"
    #     windowSpec = Window.partitionBy("turbine_id", F.window("timestamp", time_window))
    #     # Calculate both standard deviation and mean for 'power_output'
    #     self.df_with_stats = self.df_preprocessed.withColumn(
    #         "power_output_stdev",
    #         F.stddev(F.col("power_output")).over(windowSpec)
    #     ).withColumn(
    #         "power_output_mean",
    #         F.mean("power_output").over(windowSpec)
    #     )
    #
    # def compute_outliers(self):
    #     self.df_with_outliers = self.df_with_stats.withColumn(
    #         "is_outlier",
    #         F.when(
    #             F.abs(F.col("power_output") - F.col("power_output_mean")) > (2 * F.col("power_output_stdev")),
    #             1
    #         ).otherwise(0)
    #     )
    #
    # def standardise_for_imputation(self):
    #     # This method standardises the 'power_output' values before joining
    #     self.joined_df = self.df.join(
    #         self.df_with_outliers.select("turbine_id", "timestamp", "power_output_stdev", "power_output_mean",
    #                                      "is_outlier"),
    #         on=["turbine_id", "timestamp"],
    #         how="left"
    #     ).withColumn(
    #         "power_output",
    #         F.when(F.col("power_output").isin('', '0'), None).otherwise(F.col("power_output"))
    #     )
    #
    # def impute_outliers_with_mean(self):
    #
    #     self.joined_df_with_imputed = self.joined_df.withColumn("power_output_imputed",
    #                                                   F.when(F.col("power_output").isNull(),
    #                                                          F.col("power_output_mean"))
    #                                                   .otherwise(F.col("power_output")))
    #
    #     # Impute outliers with mean
    #     self.joined_df_with_imputed = self.joined_df_with_imputed.withColumn(
    #         "power_output_imputed",
    #         F.when(self.joined_df_with_imputed.is_outlier == True, self.joined_df_with_imputed.power_output_mean)
    #         .otherwise(self.joined_df_with_imputed.power_output_imputed)
    #     )
    #
    # def calculate_aggregates(self):
    #     # Defining the window spec for 24 hours partitioned by turbine_id and timestamp
    #     window_spec = Window.partitionBy("turbine_id", F.window("timestamp", "1 day"))
    #
    #     # Calculate min, max, and average power output within each window
    #     self.df_with_aggregates = self.df_preprocessed.withColumn(
    #         "min_power_output",
    #         F.min("power_output").over(window_spec)
    #     ).withColumn(
    #         "max_power_output",
    #         F.max("power_output").over(window_spec)
    #     ).withColumn(
    #         "avg_power_output",
    #         F.avg("power_output").over(window_spec)
    #     )