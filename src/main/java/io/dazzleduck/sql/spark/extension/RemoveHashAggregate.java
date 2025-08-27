package io.dazzleduck.sql.spark.extension;

import io.dazzleduck.sql.spark.ArrowRPCScan;
import org.apache.spark.sql.catalyst.rules.Rule;
import org.apache.spark.sql.execution.ProjectExec;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.aggregate.HashAggregateExec;
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec;

public class RemoveHashAggregate extends Rule<SparkPlan> {

    public RemoveHashAggregate() {

    }

    @Override
    public SparkPlan apply(SparkPlan plan) {
        return removeHashAggregate(plan);
    }

    private SparkPlan removeHashAggregate(SparkPlan plan)  {
        if(plan instanceof HashAggregateExec hashAggregateExec) {
            if(hashAggregateExec.child() instanceof ProjectExec pExec) {
                if(pExec.child() instanceof BatchScanExec bExec) {
                    if(bExec.scan() instanceof ArrowRPCScan aScan && aScan.hasPushedAggregation()){
                        System.out.println(plan.output());
                        System.out.println(pExec.output());
                        return pExec;
                    } else {
                        return plan;
                    }
                } else {
                    return plan;
                }
            } else {
                return plan;
            }
        } else {
            return plan;
        }
    }
}


