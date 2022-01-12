package minsait.ttaa.datio;

import minsait.ttaa.datio.engine.Transformer;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static minsait.ttaa.datio.common.Common.*;

public class Runner {
    static SparkSession spark;

    public static void main(String[] args) throws Exception {
        spark = SparkSession
                .builder()
                .config(config())
                .master(SPARK_MODE)
                .getOrCreate();

        Transformer engine = new Transformer(spark);
    }

    private static SparkConf config() throws Exception {
        SparkConf config = new SparkConf();

        Path path = Paths.get("src/test/resources/params");
        List<String> config_lines = Files.readAllLines(path, StandardCharsets.UTF_8);
        if (config_lines.size() < 2)
            throw new Exception("Invalid config file!");
        config.set(INPUT_PATH, config_lines.get(0));
        config.set(OUTPUT_PATH, config_lines.get(1));
        return config;
    }
}
