package io.dazzleduck.sql.spark.extension;

import org.apache.spark.sql.SparkSessionExtensions;
import org.apache.spark.sql.SparkSessionExtensionsProvider;
import scala.runtime.BoxedUnit;

public class ArrowRPCExtensions implements SparkSessionExtensionsProvider {
    @Override
    public BoxedUnit apply(SparkSessionExtensions v1) {
        v1.injectQueryStageOptimizerRule(s -> new RemoveHashAggregate());
        //v1.injectFunction();
        return BoxedUnit.UNIT;
    }
}
