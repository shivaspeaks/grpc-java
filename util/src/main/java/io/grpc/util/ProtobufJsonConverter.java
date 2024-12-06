package io.grpc.util;

import com.google.protobuf.Struct;
import com.google.protobuf.Value;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class ProtobufJsonConverter {
  private ProtobufJsonConverter() {
    // Private constructor to prevent instantiation
  }

  public static Map<String, Object> convertToJson(Struct struct) {
    Map<String, Object> result = new HashMap<>();
    for (Map.Entry<String, Value> entry : struct.getFieldsMap().entrySet()) {
      result.put(entry.getKey(), convertValue(entry.getValue()));
    }
    return result;
  }

  public static Object convertValue(Value value) {
    switch (value.getKindCase()) {
      case STRUCT_VALUE:
        return convertToJson(value.getStructValue());
      case LIST_VALUE:
        return value.getListValue().getValuesList().stream()
            .map(ProtobufJsonConverter::convertValue)
            .collect(Collectors.toList());
      case NUMBER_VALUE:
        return value.getNumberValue();
      case STRING_VALUE:
        return value.getStringValue();
      case BOOL_VALUE:
        return value.getBoolValue();
      case NULL_VALUE:
        return null;
      default:
        throw new IllegalArgumentException("Unknown Value type: " + value.getKindCase());
    }
  }
}
