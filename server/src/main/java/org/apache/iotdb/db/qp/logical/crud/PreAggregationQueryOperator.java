package org.apache.iotdb.db.qp.logical.crud;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.AlignByDevicePlan;
import org.apache.iotdb.db.qp.physical.crud.PreAggregationPlan;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.db.qp.strategy.PhysicalGenerator;

public class PreAggregationQueryOperator extends AggregationQueryOperator {

  public PreAggregationQueryOperator(QueryOperator queryOperator) {
    super(queryOperator);
  }

  @Override
  protected AlignByDevicePlan generateAlignByDevicePlan(PhysicalGenerator generator)
      throws QueryProcessException {
    AlignByDevicePlan plan = super.generateAlignByDevicePlan(generator);
    plan.setAggregationPlan(initPreAggregationPlan(new PreAggregationPlan()));
    return plan;
  }

  @Override
  public PhysicalPlan generatePhysicalPlan(PhysicalGenerator generator)
      throws QueryProcessException {
    return isAlignByDevice()
        ? this.generateAlignByDevicePlan(generator)
        : super.generateRawDataQueryPlan(
            generator, initPreAggregationPlan(new PreAggregationPlan()));
  }

  private PreAggregationPlan initPreAggregationPlan(QueryPlan queryPlan)
      throws QueryProcessException {
    PreAggregationPlan preAggregationPlan = (PreAggregationPlan) queryPlan;
    initAggregationPlan(preAggregationPlan);
    return preAggregationPlan;
  }
}
