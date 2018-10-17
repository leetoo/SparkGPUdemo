package org.apache.spark
import org.apache.spark.SparkContext
object SparkFunctions {
    def cleanFn[F <: AnyRef](sc: SparkContext, f : F ): F = {
      sc.clean(f)
    }
}
