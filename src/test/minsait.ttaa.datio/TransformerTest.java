import minsait.ttaa.datio.engine.Transformer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static minsait.ttaa.datio.common.Common.*;
import static org.junit.Assert.assertEquals;

public class TransformerTest {
    private static Dataset<Row> df;
    private static SparkSession spark;

    @BeforeClass
    public static void beforeClass() {
        spark = SparkSession
                .builder()
                .master(SPARK_MODE)
                .appName("TestingTest")
                .getOrCreate();
        df = spark.read()
                .option(HEADER, true)
                .option(INFER_SCHEMA, true)
                .csv("src/test/resources/test/filter_data.cvs");
    }

    @Test
    public void validateFilters() {
        Transformer transformer = new Transformer();
        Dataset<Row> filtered = transformer.filter(df);
        filtered.show(10, false);
        assertEquals("Must be 4", 4, filtered.count());
    }

    @AfterClass
    public static void afterClass() {
        if (spark != null) {
            spark.stop();
        }
    }
}