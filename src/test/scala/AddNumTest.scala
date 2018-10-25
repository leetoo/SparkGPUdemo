import sparkgpu._;
import sparkgpu.implicitFunc._;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkConf;
object AddNumTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("GPUTest").setMaster("local")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(12 to 15)
    val rdd2 = rdd1.addNum(9)
    val a = rdd2.collect()
    a foreach println
  }

}
