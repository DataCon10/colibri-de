from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import TimestampType


spark = SparkSession.builder.appName("Wind Turbine Data Processing").getOrCreate()

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

# First, ensure that empty strings and zeros are treated as null
# This step is crucial for uniformity before imputation
joined_df = joined_df.withColumn("power_output",
                                  F.when(F.col("power_output").isin('', '0'), None)
                                  .otherwise(F.col("power_output")))

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


