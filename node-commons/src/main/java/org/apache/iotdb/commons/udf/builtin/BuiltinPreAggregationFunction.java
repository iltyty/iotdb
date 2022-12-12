package org.apache.iotdb.commons.udf.builtin;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public enum BuiltinPreAggregationFunction {
  SUM("preagg_sum");
  private final String functionName;

  BuiltinPreAggregationFunction(String functionName) {
    this.functionName = functionName;
  }

  public String getFunctionName() {
    return functionName;
  }

  private static final Set<String> NATIVE_FUNCTION_NAMES =
      new HashSet<>(
          Arrays.stream(BuiltinPreAggregationFunction.values())
              .map(BuiltinPreAggregationFunction::getFunctionName)
              .collect(Collectors.toList()));

  public static Set<String> getNativeFunctionNames() {
    return NATIVE_FUNCTION_NAMES;
  }
}
