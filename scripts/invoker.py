from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


class WindTurbineDataProcessor:
    def __init__(self, data_paths, time_window):
        self.spark = SparkSession.builder.appName("Wind Turbine Data Processing").getOrCreate()
        self.data_paths = data_paths
        self.time_window = time_window
        self.df = self.spark.read.csv(self.data_paths, header=True, inferSchema=True)
        self.df_filtered_out_nulls = None
        self.df_preprocessed = None
        self.df_with_stats = None
        self.df_with_outliers = None
        self.joined_df = None

    # Assumes NULLs, 0s or blank values may exist due to system malfunctions. Filter out nulls first before calculating
    # averages and then later impute the NULLs based on the averages
    def filter_nulls(self):

        self.df_filtered_out_nulls = self.df.filter(self.df.power_output.isNotNull() &
                                    (self.df.power_output != 0) &
                                    (self.df.power_output != ''))

    # Ensures timestamps are in the correct format
    def process_ts(self):

        self.df_preprocessed = self.df_filtered_out_nulls.withColumn("timestamp", F.to_timestamp(F.col("timestamp"), "dd/MM/yyyy HH:mm:ss"))

    # Computes stdev and mean after NULLs have been removed, ensuring results aren't skewed
    def compute_stats(self, time_window):
        # Define the window specification e.g. "1 day"
        windowSpec = Window.partitionBy("turbine_id", F.window("timestamp", time_window))
        # Calculate both standard deviation and mean for 'power_output'
        self.df_with_stats = self.df_preprocessed.withColumn(
            "power_output_stdev",
            F.stddev(F.col("power_output")).over(windowSpec)
        ).withColumn(
            "power_output_mean",
            F.mean("power_output").over(windowSpec)
        )

    def compute_outliers(self):
        self.df_with_outliers = self.df_with_stats.withColumn(
            "is_outlier",
            F.when(
                F.abs(F.col("power_output") - F.col("power_output_mean")) > (2 * F.col("power_output_stdev")),
                1
            ).otherwise(0)
        )

    def standardise_for_imputation(self):
        # This method standardises the 'power_output' values before joining
        self.joined_df = self.df.join(
            self.df_with_outliers.select("turbine_id", "timestamp", "power_output_stdev", "power_output_mean",
                                         "is_outlier"),
            on=["turbine_id", "timestamp"],
            how="left"
        ).withColumn(
            "power_output",
            F.when(F.col("power_output").isin('', '0'), None).otherwise(F.col("power_output"))
        )

    def impute_outliers_with_mean(self):
        self.joined_df = self.joined_df.withColumn(
            "power_output_imputed",
            F.when(self.joined_df.is_outlier, self.joined_df.power_output_mean)
            .otherwise(F.col("power_output"))
        )

    def calculate_aggregates(self):
        # Defining the window spec for 24 hours partitioned by turbine_id and timestamp
        window_spec = Window.partitionBy("turbine_id", F.window("timestamp", "1 day"))

        # Calculate min, max, and average power output within each window
        self.df_with_aggregates = self.df_preprocessed.withColumn(
            "min_power_output",
            F.min("power_output").over(window_spec)
        ).withColumn(
            "max_power_output",
            F.max("power_output").over(window_spec)
        ).withColumn(
            "avg_power_output",
            F.avg("power_output").over(window_spec)
        )





data = ["data/data_group_1.csv",
        "data/data_group_2.csv",
        "data/data_group_3.csv"]
df = spark.read.csv(data, header=True, inferSchema=True)

# Handle outliers 1st to avoid them skewing imputation statistics for NULLs
# Downside = null values may impact the result of handling outliers
# - strategy could be to ignore nulls when fixing for outliers

# 1. Calculate mean and standard deviation of power_output for each turbine, excluding NULLs
# Filter out rows with NULL values in the power_output column

df_filtered = df.filter(df.power_output.isNotNull() &
                        (df.power_output != 0) &
                        (df.power_output != ''))


df_filtered = df_filtered.withColumn("timestamp", F.to_timestamp(F.col("timestamp"), "dd/MM/yyyy HH:mm:ss"))


# Define a window spec for 24 hours partitioned by turbine_id
windowSpec = Window.partitionBy("turbine_id", F.window("timestamp", "1 day"))

# Calculate standard deviation within each window, excluding NULL values
df_with_stdev = df_filtered.withColumn("power_output_stdev",
                                       F.stddev(F.col("power_output")).over(windowSpec))

# Calculate mean power output for each turbine for each day
df_with_mean = df_with_stdev.withColumn(
    "power_output_mean",
    F.mean('power_output').over(windowSpec)
)

# Identify outliers (more than 2 standard deviations from the mean)
df_with_outliers = df_with_mean.withColumn(
    "is_outlier",
    F.when(
        F.abs(df_with_mean.power_output - df_with_mean.power_output_mean) > (2 * df_with_mean.power_output_stdev),
        1
    ).otherwise(0)
)


# Left Join original DF back onto df with computed outliers to impute mean for possible NULL/missing values
joined_df =  df.join(df_with_outliers.select('turbine_id', 'timestamp', "power_output_stdev",
                                             "power_output_mean", "is_outlier"),
                     on=["turbine_id", "timestamp"],
                     how="left")


# 2. Impute NULL values excluding outliers
# Assuming 'mean_power_output' is already in your DataFrame,
# if not, calculate it as needed.
# This is a simplified example showing how to replace NULL, empty string, or 0
# with the mean value in the 'power_output' column.



# Now, impute missing values with the mean
joined_df_with_imputed = joined_df.withColumn("power_output_imputed",
                                               F.when(F.col("power_output").isNull(),
                                                      F.col("power_output_mean"))
                                               .otherwise(F.col("power_output")))

# Impute outliers with mean

updated_df = joined_df_with_imputed.withColumn(
    "power_output_imputed",
    F.when(joined_df_with_imputed.is_outlier == True, joined_df_with_imputed.power_output_mean)
    .otherwise(joined_df_with_imputed.power_output_imputed)
)



print('done')


