package minsait.ttaa.datio.engine;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.jetbrains.annotations.NotNull;

import static minsait.ttaa.datio.common.Common.*;
import static minsait.ttaa.datio.common.naming.PlayerInput.*;
import static minsait.ttaa.datio.common.naming.PlayerOutput.*;
import static org.apache.spark.sql.functions.*;

public class Transformer extends Writer {
    private SparkSession spark;

    public Transformer() {

    }

    public Transformer(@NotNull SparkSession spark) {
        this.spark = spark;
        Dataset<Row> df = readInput();

        df.printSchema();

        df = cleanData(df);
        df = rangeAge(df);
        df = rankByNationalityPosition(df);
        df = potentialVsOverall(df);
        df = filter(df);
        df = columnSelection(df);

        // for show 100 records after your transformations and show the Dataset schema
        df.show(100, false);
        df.printSchema();

        // Uncomment when you want write your final output
        //write(df);
    }

    private Dataset<Row> columnSelection(Dataset<Row> df) {
        return df.select(
                shortName.column(),
                longName.column(),
                age.column(),
                heightCm.column(),
                weightCm.column(),
                nationality.column(),
                club_name.column(),
                overall.column(),
                potential.column(),
                teamPosition.column(),
                ageRange.column(),
                rankNationalityPostionByOverall.column(),
                potentialVsOverall.column()
        );
    }

    /**
     * @return a Dataset readed from csv file
     */
    private Dataset<Row> readInput() {
        Dataset<Row> df = spark.read()
                .option(HEADER, true)
                .option(INFER_SCHEMA, true)
                .csv(INPUT_PATH);
        return df;
    }

    /**
     * @param df
     * @return a Dataset with filter transformation applied
     * column team_position != null && column short_name != null && column overall != null
     */
    private Dataset<Row> cleanData(Dataset<Row> df) {
        df = df.filter(
                teamPosition.column().isNotNull().and(
                        shortName.column().isNotNull()
                ).and(
                        overall.column().isNotNull()
                )
        );

        return df;
    }

    /**
     * @param df is a Dataset with players information (must have team_position and height_cm columns)
     * @return add to the Dataset the column "cat_height_by_position"
     * by each position value
     * cat A for if is in 20 players tallest
     * cat B for if is in 50 players tallest
     * cat C for the rest
     */
    private Dataset<Row> exampleWindowFunction(Dataset<Row> df) {
        WindowSpec w = Window
                .partitionBy(teamPosition.column())
                .orderBy(heightCm.column().desc());

        Column rank = rank().over(w);

        Column rule = when(rank.$less(10), "A")
                .when(rank.$less(50), "B")
                .otherwise("C");

        df = df.withColumn(catHeightByPosition.getName(), rule);

        return df;
    }

    /**
     * @param df is a Dataset with players information (must have age column)
     * @return add to the Dataset the column "age_range"
     * by each position value
     * cat A if age is less than 23
     * cat B if age is greater than or equal to 23 and less than 27
     * cat C if age is greater than or equal to 27 and less than 32
     * cat D for the rest
     */
    private Dataset<Row> rangeAge(Dataset<Row> df) {

        Column rule = when(age.column().$less(23), "A")
                .when(age.column().$greater$eq(23).and(age.column().$less(27)), "B")
                .when(age.column().$greater$eq(27).and(age.column().$less(32)), "C")
                .otherwise("D");

        df = df.withColumn(ageRange.getName(), rule);

        return df;
    }

    /**
     * @param df is a Dataset with players information (must have nationality, teamPosition and overall columns)
     * @return add to the Dataset the column "rank_by_nationality_position"
     * ranks players by nationality and position according to overall stat
     */
    private Dataset<Row> rankByNationalityPosition(Dataset<Row> df) {
        WindowSpec w = Window
                .partitionBy(nationality.column(), teamPosition.column())
                .orderBy(overall.column().desc());

        Column rank = row_number().over(w);

        df = df.withColumn(rankNationalityPostionByOverall.getName(), rank);

        return df;
    }

    /**
     * @param df is a Dataset with players information (must have potential and overall columns)
     * @return add to the Dataset the column "potential_vs_overall"
     * calculates the potential/overall
     */
    private Dataset<Row> potentialVsOverall(Dataset<Row> df) {
        Column rule = potential.column().divide(overall.column());
        df = df.withColumn(potentialVsOverall.getName(), rule);
        return df;
    }

    /**
     * @param df is a Dataset with players information (must have age_range, rank_by_nationality_position
     *           and potential_vs_overall columns)
     * @return a filtered Dataset with the following conditions:
     * rank_by_nationality_position < 3
     * age_range in [B, C] and potential_vs_overall > 1.15
     * age_range equals A and potential_vs_overall > 1.25
     * age_range equals D and rank_by_nationality_position < 5
     */
    public Dataset<Row> filter(Dataset<Row> df) {
        Column ageCat = ageRange.column();
        Column potentialVSOverall = potentialVsOverall.column();
        Column nationalityPosRank = rankNationalityPostionByOverall.column();

        Column rankCondition = nationalityPosRank.$less(3);
        Column ageRangeBorCAndPontentialVsOverallSup115 =
                ageCat.isin("A", "B").and(potentialVSOverall.$greater(1.15));
        Column ageEqAAndPotentialVsOverallSup125 =
                ageCat.$eq$eq$eq("A").and(potentialVSOverall.$greater(1.25));
        Column ageEqDAndRankByNationalityPositionLessThan5 =
                ageCat.$eq$eq$eq("D").and(nationalityPosRank.$less(5));

        return df.filter(
                rankCondition
                        .or(ageRangeBorCAndPontentialVsOverallSup115)
                        .or(ageEqAAndPotentialVsOverallSup125)
                        .or(ageEqDAndRankByNationalityPositionLessThan5)
        );
    }

}
