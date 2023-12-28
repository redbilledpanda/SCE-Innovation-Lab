# Imports
import findspark, pyspark, os, pandas as pd, pyspark.errors as E  # noqa: E401
from IPython.display import display
from pyspark.sql import (
    functions as F,
    types as T,
    Window as W,
    DataFrame as DF,
)


class SparkApplication():
    # variable declaration
    sparkSession = pyspark.sql.SparkSession
    sc = pyspark.SparkContext
    app_name = str
    start_date = None
    end_date = None

    def __init__(self, app_name="Silo_Report"):
        self.app_name = app_name
        # Initialize Spark and set log level
        findspark.init()

    def __enter__(self):
        conf = (
            pyspark.SparkConf()
            .setAppName(self.app_name)
            .setMaster("local[*]")
            # .set("spark.driver.memory", "512m")
            # .set("spark.executor.memoryOverhead", "384m")
            # .set("spark.executor.memory", "1g")
            # .set("spark.executor.pyspark.memory", "512m")
        )
        self.sc = pyspark.SparkContext(conf=conf)
        self.sc.setLogLevel("ERROR")  # since this a notebook code, we don't want to see too warning bad practices logs
        self.sparkSession = pyspark.sql.SparkSession(self.sc)
        return self

    def __exit__(self, *args):
        self.sc.stop()

    def get_dataframes(self) -> tuple[DF, DF]:
        '''
            Returns the two dataframes:

            - Keyword Arguemnts:

                - `spark` -> gets or creates an instance of `pyspark.sql.SparkSession` if not passed in

            - Returns:
                - a tuple of two `pyspark.sql.DataFrame` objects
                    - `df_historical` -> `pyspark.sql.DataFrame` of historical averages
                    - `df_actual` -> `pyspark.sql.DataFrame` of actuals
        '''
        df_historical = self.sparkSession.read.csv("assets/data/historical_averages.csv", header=True)
        df_actual = (
            self.sparkSession.read.csv("assets/data/silo_actuals.csv", header=True)
            .withColumn(
                "date",
                F.to_date(
                    F.to_timestamp(F.col("date"), 'M/d/yyyy')
                ).cast(T.DateType())
            )
        )
        return df_historical, df_actual

    def backfill_dates(self, df_actual=DF) -> DF:
        '''
            SPARK CHALLENGE: DO NOT use sequential for loops to backfill dates

            ANSWER:
            Returns a `pyspark.sql.DataFrame` of backfilled dates between the minimum and maximum dates in `df_actual`
            using `pyspark.sql.functions.sequence` and `pyspark.sql.functions.explode`
            - Keyword Arguments:
                - `df_actual` -> `pyspark.sql.DataFrame` of actuals

            - Returns:
                - `pyspark.sql.DataFrame` of dates between the minimum and maximum dates in `df_actual`
        '''
        (self.start_date, self.end_date) = df_actual.agg(F.min('date'), F.max('date')).collect()[0][:]  # noqa
        df_dates = (
            df_actual
            .sparkSession
            .createDataFrame([(self.start_date, self.end_date)], ["start_date", "end_date"])
        )
        df_dates = (
            df_dates
            .selectExpr("explode(sequence(to_date(start_date), to_date(end_date))) as date")
        )
        return df_dates

    # ## TRANFORM GENERATOR FUNCTIONS ##
    def get_date_related_columns(self, orig_df=DF) -> DF:
        '''
            Returns a `pyspark.sql.DataFrame` with the bunch date related columns added:
            based off of the `date` column

            if `'date'` column is not found in `orig_df`, raises `pyspark.errors.SparkRuntimeException`

            Returns following columns added to `orig_df`:
            - `day` - day of the week (int)
            - `day_of_week` - day of the week (int)
            - `month` - month of the year (int)
            - `week_no` - week number of the year (int)
        '''
        if 'date' not in orig_df.columns:
            raise E.SparkRuntimeException("Column 'date' not found in this dataframe")
        return (
            orig_df
            .withColumn("day", F.date_format("date", 'EEEE'))
            .withColumn("day_of_week", F.dayofweek("date"))
            .withColumn("month", F.month("date"))
            .withColumn("week_no", F.weekofyear("date"))
        )

    def merge_historical_and_actual(self, orig_df=DF, df_his=DF, df_act=DF) -> DF:
        '''
        Merges `df_his` and `df_act` with `orig_df` and returns a `pyspark.sql.DataFrame`
        '''
        return (
            orig_df
            .join(df_act, on="date", how="left")
            .join(df_his, on="day", how="left")
        )

    def get_silo_wt_in_tons(self, orig_df=DF) -> DF:
        '''
        returns a `pyspark.sql.DataFrame` with the `'silo_wt_in_tons'` column added based on the following logic:
        - if `silo_wt_in_tons` from `df_actual` is availabe, use it
        - else use `average_tons` column from `df_historical`
        '''
        return (
            orig_df
            .withColumn("silo_wt_in_tons", F.coalesce(F.col("silo_wt_in_tons"), F.col("average_tons")))
        )

    def reassign_week_no(self, orig_df=DF) -> DF:
        '''
        returns a `pyspark.sql.DataFrame` with the reassigned `'week_no'`
        and `'day_of_week'` based on a week that starts from thursday and ends on wednesday:

        - `'day_of_week_new'`:
            - if `day_of_week` minus 4 is greater than 1, then use `day_of_week` minus 4
            - else use `day_of_week` plus 3
        - `'week_no_new'`:
            - if `day_of_week_new` is between 5 and 7, subtract 1 from `week_no`
            - else keep `week_no` as is
        '''
        return (
            orig_df
            .withColumn(
                "day_of_week_new",
                F.when(
                    (F.col("day_of_week") - F.lit(4)) >= 1, (F.col("day_of_week") - F.lit(4))
                ).otherwise(
                    (F.col("day_of_week") + F.lit(3))
                )
            )
            .withColumn(
                "week_no_new",
                F.when(
                    (F.col("day_of_week_new") >= 5) & (F.col("day_of_week_new") <= 7),
                    (F.col("week_no") - F.lit(1))
                ).otherwise(F.col("week_no"))
            )
        )

    def get_weekly_total_tons(self, orig_df=DF) -> DF:
        '''
        SPARK CHALLENGE: DO NOT use seperate datset to generate weekly totals

        ANSWER: returns a `pyspark.sql.DataFrame` with the `'weekly_total_tons'`
        column added based on the following logic:
        - Generate a running total of `silo_wt_in_tons` column over a window of `week_no_new` and `day_of_week_new`
        - if `day_of_week_new` is 7, then use `weekly_total_tons` column
        - else use `None`
        '''
        weekly_window = (W.partitionBy("week_no_new").orderBy("day_of_week_new").rangeBetween(W.unboundedPreceding, 0))
        return (
            orig_df
            .withColumn("weekly_total_tons", F.sum("silo_wt_in_tons").over(weekly_window))
            .withColumn(
                "weekly_total_tons",
                F.when(F.col("day_of_week_new") == 7, F.col("weekly_total_tons"))
                .otherwise(F.lit(None))
            )
        )

    def get_monthly_report_columns(self, orig_df=DF) -> DF:
        '''
        SPARK CHALLENGE: DO NOT use seperate datset

        ANSWER: returns a `pyspark.sql.DataFrame` with the following columns added:
        - `mtd_running_total_tons` - running total of `silo_wt_in_tons` column over a window of `month`
        - `monthly_grand_total` - if `date` is equal to `end_date`, then use `weekly_total_tons` column
        '''
        # .strptime(f'%m/%d/%Y')
        monthly_window = (W.partitionBy("month").orderBy("date").rangeBetween(W.unboundedPreceding, 0))
        return (
            orig_df
            .withColumn("mtd_running_total_tons", F.sum("silo_wt_in_tons").over(monthly_window))
            .withColumn(
                "monthly_grand_total",
                F.when(F.col("date") == self.end_date, F.col("mtd_running_total_tons"))
                .otherwise(F.lit(None))
            )
        )

    def select_column_order(self, orig_df=DF) -> DF:
        '''
        returns a `pyspark.sql.DataFrame` with the following columns selected and ordered:
        - `date`
        - `silo_wt_in_tons`
        - `weekly_total_tons`
        - `mtd_running_total_tons`
        - `monthly_grand_total`
        '''
        return (
            orig_df
            .sort("date", ascending=True)
            .select(
                "date",
                "silo_wt_in_tons",
                "weekly_total_tons",
                "mtd_running_total_tons",
                "monthly_grand_total"
            )
        )

    def write_to_filesystem(self, write_df=DF, write_location=str):
        '''
        writes `write_df` to `write_location` as a CSV file'''
        write_df.coalesce(1).write.csv(write_location, header=True, mode="overwrite")

    def print_dataframe(self, df_location=str) -> int:
        '''
        prints the final output dataframe of the first CSV file found in `df_location` on IPython console
        - Keyword Arguments:
            - `df_location` -> location of the CSV file
        - Returns:
            - `0` if successful
        '''
        for file in os.listdir(df_location):
            if file.endswith(".csv"):
                df = pd.read_csv(os.path.join(df_location, file), sep=",", keep_default_na=False)
                # shows top 10 rows
                display(df.head(32))
                return 0
        raise FileNotFoundError("No CSV files found in {folder}".format(folder=df_location))
