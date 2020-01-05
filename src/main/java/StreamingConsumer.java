import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class StreamingConsumer {
    public static void main(String[] args) throws StreamingQueryException {
        System.setProperty("hadoop.home.dir","C:\\Users\\talhaklc\\Desktop\\hadoop-common-2.2.0-bin-master");
        SparkSession sparkSession=SparkSession.builder().appName("streaming-time-op").master("local").getOrCreate();

        StructType schema = new StructType().add("product",
                "string").add("time", DataTypes.TimestampType);

        Dataset<SearchProductModel> ds = sparkSession.readStream().format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "search").load()
                .selectExpr("CAST(value AS STRING) as message")
                .select(functions.from_json(functions.col("message"), schema).as("json")).select("json.*")
                .as(Encoders.bean(SearchProductModel.class));

        Dataset<Row> windowedCounts = ds
                .groupBy(functions.window(ds.col("time"), "1 minute"), ds.col("product")).count();

        StreamingQuery query = windowedCounts.writeStream().outputMode("complete").format("console").start();
        query.awaitTermination();
    }
}
