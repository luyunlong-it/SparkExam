import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.functions._

class DataFrameOperationsTest extends AnyFunSuite with BeforeAndAfter {

  var spark: SparkSession = _

  before {
    // 在测试之前初始化SparkSession
    spark = SparkSession.builder()
      .master("local[*]")
      .appName("DataFrameOperationsTest")
      .getOrCreate()
  }

  after {
    // 测试完成后停止SparkSession
    if (spark != null) {
      spark.stop()
    }
  }

  test("AddNewColumn function adds a new column with the correct value") {
    import spark.implicits._

    // 创建测试DataFrame
    val initialDF = Seq(
      (1, "Alice"),
      (2, "Bob")
    ).toDF("id", "name")

    // 应用我们的操作
    val newColumnName = "age"
    val resultDF = DataFrameOperations.addNewColumn(initialDF, newColumnName, 30)

    // 验证新列是否存在于结果DataFrame中
    assert(resultDF.columns.contains(newColumnName))

    // 验证新列的值是否符合预期
    resultDF.collect().foreach {
      row => assert(row.getAs[Int](newColumnName) == 30)
    }
  }
}