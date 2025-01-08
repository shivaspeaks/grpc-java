package io.grpc.xds;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Struct;
import io.envoyproxy.envoy.config.core.v3.Address;
import io.envoyproxy.envoy.config.core.v3.Metadata;
import io.envoyproxy.envoy.config.core.v3.SocketAddress;
import io.envoyproxy.envoy.extensions.filters.http.gcp_authn.v3.Audience;
import io.grpc.xds.MetadataRegistry.MetadataValueParser;
import io.grpc.xds.internal.ProtobufJsonConverter;
import java.util.Map;

public class MetadataParser {

  /**
   * Parses cluster metadata into a structured map.
   *
   * <p>Values in {@code typed_filter_metadata} take precedence over
   * {@code filter_metadata} when keys overlap, following Envoy API behavior. See
   * <a href="https://github.com/envoyproxy/envoy/blob/main/api/envoy/config/core/v3/base.proto#L217-L259">
   *   Envoy metadata documentation </a> for details.
   *
   * @param metadata the {@link Metadata} containing the fields to parse.
   * @return an immutable map of parsed metadata.
   * @throws InvalidProtocolBufferException if parsing {@code typed_filter_metadata} fails.
   */
  public static ImmutableMap<String, Object> parseMetadata(Metadata metadata)
      throws InvalidProtocolBufferException {
    ImmutableMap.Builder<String, Object> parsedMetadata = ImmutableMap.builder();

    MetadataRegistry registry = MetadataRegistry.getInstance();
    // Process typed_filter_metadata
    for (Map.Entry<String, Any> entry : metadata.getTypedFilterMetadataMap().entrySet()) {
      String key = entry.getKey();
      Any value = entry.getValue();
      MetadataValueParser parser = registry.findParser(value.getTypeUrl());
      if (parser != null) {
        Object parsedValue = parser.parse(value);
        parsedMetadata.put(key, parsedValue);
      }
    }
    // building once to reuse in the next loop
    ImmutableMap<String, Object> intermediateParsedMetadata = parsedMetadata.build();

    // Process filter_metadata for remaining keys
    for (Map.Entry<String, Struct> entry : metadata.getFilterMetadataMap().entrySet()) {
      String key = entry.getKey();
      if (!intermediateParsedMetadata.containsKey(key)) {
        Struct structValue = entry.getValue();
        Object jsonValue = ProtobufJsonConverter.convertToJson(structValue);
        parsedMetadata.put(key, jsonValue);
      }
    }

    return parsedMetadata.build();
  }

  static class AudienceMetadataParser implements MetadataValueParser {

    @Override
    public String getTypeUrl() {
      return "type.googleapis.com/envoy.extensions.filters.http.gcp_authn.v3.Audience";
    }

    @Override
    public String parse(Any any) throws InvalidProtocolBufferException {
      Audience audience = any.unpack(Audience.class);
      String url = audience.getUrl();
      if (url.isEmpty()) {
        throw new InvalidProtocolBufferException(
            "Audience URL is empty. Metadata value must contain a valid URL.");
      }
      return url;
    }
  }

  static class AddressMetadataParser implements MetadataValueParser {

    @Override
    public String getTypeUrl() {
      return "type.googleapis.com/envoy.config.core.v3.Address";
    }

    @Override
    public String parse(Any any) throws InvalidProtocolBufferException {
      Address address = any.unpack(Address.class);
      SocketAddress socketAddress = address.getSocketAddress();
      if (socketAddress.getAddress().isEmpty()) {
        throw new InvalidProtocolBufferException("Address field is empty or invalid.");
      }

      String ip = socketAddress.getAddress();
      int port = socketAddress.getPortValue();
      if (port <= 0) {
        throw new InvalidProtocolBufferException("Port value must be positive.");
      }

      return String.format("%s:%d", ip, port);
    }
  }
}
