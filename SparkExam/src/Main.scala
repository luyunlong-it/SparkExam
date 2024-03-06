import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object PeerYearCalculationApp extends App {
  val spark = SparkSession.builder()
    .appName("Peer Year Calculation")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  // 定义Spark会话
  implicit val session: SparkSession = spark

  // 示例数据
  val data = Seq(
    ("ABC17969(AB)", "1", "ABC17969", 2022),
    ("ABC17969(AB)", "2", "CDC52533", 2022),
    ("ABC17969(AB)", "3", "DEC59161", 2023),
    ("ABC17969(AB)", "4", "F43874", 2022),
    ("ABC17969(AB)", "5", "MY06154", 2021),
    ("ABC17969(AB)", "6", "MY4387", 2022),

    ("AE686(AE)", "7", "AE686", 2023),
    ("AE686(AE)", "8", "BH2740", 2021),
    ("AE686(AE)", "9", "EG999", 2021),
    ("AE686(AE)", "10", "AE0908", 2021),
    ("AE686(AE)", "11", "QA402", 2022),
    ("AE686(AE)", "12", "OM691", 2022)
  ).toDF("peer_id", "id_1", "id_2", "year")

  val givenSize = 3

  processData(data, givenSize).show()

  def processData(df: DataFrame, size: Int)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    // 步骤1: 找到每个peer_id对应的interest年份
    val df_with_interest_year = df
      .join(df.select($"peer_id".as("peer_id_join"), $"year".as("interest_year")).distinct(), $"peer_id" === $"peer_id_join" && $"id_2" === substring($"peer_id", 1, length($"id_2")), "left")
      .drop("peer_id_join")
      .na.drop(Seq("interest_year"))  // 去除没有匹配interest年份的行

    // 步骤2&3: 计算次数，并实现滤过逻辑
    val df_count = df_with_interest_year.groupBy($"peer_id", $"year")
      .count()
      .filter($"year" <= $"interest_year")
      .orderBy($"peer_id", $"year".desc)

    df_count
  }
}