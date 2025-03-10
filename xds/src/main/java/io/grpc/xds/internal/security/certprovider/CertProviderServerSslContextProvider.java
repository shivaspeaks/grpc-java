/*
 * Copyright 2020 The gRPC Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.grpc.xds.internal.security.certprovider;

import static com.google.common.base.Preconditions.checkNotNull;

import io.envoyproxy.envoy.config.core.v3.Node;
import io.envoyproxy.envoy.extensions.transport_sockets.tls.v3.CertificateValidationContext;
import io.envoyproxy.envoy.extensions.transport_sockets.tls.v3.CommonTlsContext;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.xds.EnvoyServerProtoData.DownstreamTlsContext;
import io.grpc.xds.client.Bootstrapper.CertificateProviderInfo;
import io.grpc.xds.internal.security.trust.XdsTrustManagerFactory;
import io.netty.handler.ssl.SslContextBuilder;
import java.io.IOException;
import java.security.cert.CertStoreException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Map;
import javax.annotation.Nullable;

/** A server SslContext provider using CertificateProviderInstance to fetch secrets. */
final class CertProviderServerSslContextProvider extends CertProviderSslContextProvider {

  CertProviderServerSslContextProvider(
      Node node,
      @Nullable Map<String, CertificateProviderInfo> certProviders,
      CommonTlsContext.CertificateProviderInstance certInstance,
      CommonTlsContext.CertificateProviderInstance rootCertInstance,
      CertificateValidationContext staticCertValidationContext,
      DownstreamTlsContext downstreamTlsContext,
      CertificateProviderStore certificateProviderStore) {
    super(
        node,
        certProviders,
        checkNotNull(certInstance, "Server SSL requires certInstance"),
        rootCertInstance,
        staticCertValidationContext,
        downstreamTlsContext,
        certificateProviderStore);
  }

  @Override
  protected final SslContextBuilder getSslContextBuilder(
      CertificateValidationContext certificateValidationContextdationContext)
      throws CertStoreException, CertificateException, IOException {
    SslContextBuilder sslContextBuilder = SslContextBuilder.forServer(savedKey, savedCertChain);
    XdsTrustManagerFactory trustManagerFactory = null;
    if (isMtls() && savedSpiffeTrustMap != null) {
      trustManagerFactory = new XdsTrustManagerFactory(
          savedSpiffeTrustMap,
          certificateValidationContextdationContext);
    } else if (isMtls()) {
      trustManagerFactory = new XdsTrustManagerFactory(
          savedTrustedRoots.toArray(new X509Certificate[0]),
          certificateValidationContextdationContext);
    }
    setClientAuthValues(sslContextBuilder, trustManagerFactory);
    sslContextBuilder = GrpcSslContexts.configure(sslContextBuilder);
    return sslContextBuilder;
  }

}
