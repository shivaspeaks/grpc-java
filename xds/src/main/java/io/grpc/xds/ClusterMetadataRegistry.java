package io.grpc.xds;

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import io.envoyproxy.envoy.extensions.filters.http.gcp_authn.v3.Audience;
import java.util.HashMap;
import java.util.Map;

public class ClusterMetadataRegistry {
  private static final ClusterMetadataRegistry INSTANCE = new ClusterMetadataRegistry();

  private final Map<String, ClusterMetadataValueParser> supportedParsers = new HashMap<>();

  private ClusterMetadataRegistry() {
    registerParsers(
        new Object[][]{
            {"extensions.filters.http.gcp_authn.v3.Audience", new AudienceMetadataParser()},
            // Add more parsers here as needed
        });
  }

  public static ClusterMetadataRegistry getInstance() {
    return INSTANCE;
  }

  public ClusterMetadataValueParser findParser(String typeUrl) {
    return supportedParsers.get(typeUrl);
  }

  private void registerParsers(Object[][] parserEntries) {
    for (Object[] entry : parserEntries) {
      String typeUrl = (String) entry[0];
      ClusterMetadataValueParser parser = (ClusterMetadataValueParser) entry[1];
      supportedParsers.put(typeUrl, parser);
    }
  }

  @FunctionalInterface
  public interface ClusterMetadataValueParser {
    Object parse(Any any) throws InvalidProtocolBufferException;
  }

  /**
   * Parser for Audience metadata type.
   */
  public static class AudienceMetadataParser implements ClusterMetadataValueParser {
    @Override
    public String parse(Any any) throws InvalidProtocolBufferException {
      if (any.is(Audience.class)) {
        Audience audience = any.unpack(Audience.class);
        String url = audience.getUrl();
        if (url.isEmpty()) {
          throw new InvalidProtocolBufferException("Audience URL is empty.");
        }
        return url;
      } else {
        throw new InvalidProtocolBufferException("Unexpected message type: " + any.getTypeUrl());
      }
    }
  }
}
