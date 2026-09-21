/*
 * Copyright 2026 The gRPC Authors
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

package io.grpc.autosharding;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.fail;

import com.google.cloud.autosharding.v1.AssignmentChunk;
import com.google.cloud.autosharding.v1.AssignmentMetadata;
import com.google.cloud.autosharding.v1.AutoshardingServiceGrpc;
import com.google.cloud.autosharding.v1.EndpointState;
import com.google.cloud.autosharding.v1.LoadReportingConfig;
import com.google.cloud.autosharding.v1.PerSliceEndpointState;
import com.google.cloud.autosharding.v1.SliceAssignment;
import com.google.cloud.autosharding.v1.WatchShardingAssignmentRequest;
import com.google.cloud.autosharding.v1.WatchShardingAssignmentResponse;
import com.google.protobuf.ByteString;
import io.grpc.Channel;
import io.grpc.Status;
import io.grpc.SynchronizationContext;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.internal.BackoffPolicy;
import io.grpc.internal.FakeClock;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link AutoshardingClient}. */
@RunWith(JUnit4.class)
public class AutoshardingClientTest {
  private static final String CLIENT_UUID = "client-uuid-1";
  private static final String TARGET = "autosharding-target";
  private static final String OTHER_TARGET = "other-autosharding-target";
  private static final long TIMEOUT_SECONDS = 5;
  private static final long BACKOFF_NANOS = TimeUnit.SECONDS.toNanos(1);

  @Rule public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  private final SynchronizationContext syncContext =
      new SynchronizationContext(
          (t, e) -> {
            throw new AssertionError(e);
          });
  private final FakeClock fakeClock = new FakeClock();
  private final FakeAutoshardingService service = new FakeAutoshardingService();
  private final BlockingQueue<Assignment> assignments = new LinkedBlockingQueue<>();
  private final RecordingBackoffPolicyProvider backoffPolicyProvider =
      new RecordingBackoffPolicyProvider();

  private Channel channel;
  private AutoshardingClient client;

  @Before
  public void setUp() throws Exception {
    String serverName = InProcessServerBuilder.generateName();
    grpcCleanup.register(
        InProcessServerBuilder.forName(serverName)
            .directExecutor()
            .addService(service)
            .build()
            .start());
    channel =
        grpcCleanup.register(
            InProcessChannelBuilder.forName(serverName).directExecutor().build());
    client =
        new AutoshardingClient(
            CLIENT_UUID,
            syncContext,
            fakeClock.getScheduledExecutorService(),
            backoffPolicyProvider,
            fakeClock.getStopwatchSupplier(),
            assignments::add);
  }

  @After
  public void tearDown() {
    // Must happen before GrpcCleanupRule shuts the channel down, otherwise the client keeps
    // retrying against a terminating channel.
    syncContext.execute(client::shutdown);
  }

  @Test
  public void update_startsStreamAndSendsInitialClientConfig() throws Exception {
    update(channel, TARGET);

    WatchShardingAssignmentRequest request = takeRequest();
    assertThat(request.hasInitialClientConfig()).isTrue();
    assertThat(request.getInitialClientConfig().getTarget()).isEqualTo(TARGET);
    assertThat(request.getInitialClientConfig().getClientUuid()).isEqualTo(CLIENT_UUID);
    assertThat(request.getInitialClientConfig().getLatestGeneration()).isEqualTo(0);
  }

  @Test
  public void update_unchanged_doesNotRestartStream() throws Exception {
    update(channel, TARGET);
    takeRequest();

    update(channel, TARGET);

    assertThat(service.streamCount.get()).isEqualTo(1);
  }

  @Test
  public void chunksBufferedUntilMetadata_thenAssignmentDeliveredAndAcked() throws Exception {
    update(channel, TARGET);
    takeRequest();
    StreamObserver<WatchShardingAssignmentResponse> serverStream = takeServerStream();

    serverStream.onNext(chunkResponse(chunkWithEndpoint("host-a", "", null, 0)));
    assertThat(assignments).isEmpty();
    assertThat(service.requests).isEmpty();

    serverStream.onNext(metadataResponse(5));

    Assignment assignment = takeAssignment();
    assertThat(assignment.getGeneration()).isEqualTo(5);
    assertThat(assignment.getEndpointNames()).containsExactly("host-a");
    assertThat(assignment.getSlices()).hasSize(1);

    WatchShardingAssignmentRequest ack = takeRequest();
    assertThat(ack.hasAssignmentAck()).isTrue();
    assertThat(ack.getAssignmentAck().getGeneration()).isEqualTo(5);
    assertThat(ack.getAssignmentAck().getAccepted()).isTrue();
    assertThat(ack.getAssignmentAck().getErrorMessage()).isEmpty();
  }

  @Test
  public void multipleChunks_combinedIntoOneLogicalAssignment() throws Exception {
    update(channel, TARGET);
    takeRequest();
    StreamObserver<WatchShardingAssignmentResponse> serverStream = takeServerStream();

    serverStream.onNext(
        chunkResponse(AssignmentChunk.newBuilder().addEndpoints(endpoint("host-a")).build()));
    serverStream.onNext(
        chunkResponse(
            AssignmentChunk.newBuilder()
                .addEndpoints(endpoint("host-b"))
                .addSliceAssignments(sliceAssignment("", null, 1))
                .build()));
    serverStream.onNext(metadataResponse(1));

    Assignment assignment = takeAssignment();
    assertThat(assignment.getEndpointNames()).containsExactly("host-a", "host-b").inOrder();
    assertThat(assignment.getSlices().get(0).getEndpoints()).containsExactly(1);
  }

  @Test
  public void invalidAssignment_nackedAndNotDelivered() throws Exception {
    update(channel, TARGET);
    takeRequest();
    StreamObserver<WatchShardingAssignmentResponse> serverStream = takeServerStream();

    // Endpoint index 3 does not exist in the combined endpoint list.
    serverStream.onNext(chunkResponse(chunkWithEndpoint("host-a", "", null, 3)));
    serverStream.onNext(metadataResponse(5));

    WatchShardingAssignmentRequest nack = takeRequest();
    assertThat(nack.hasAssignmentAck()).isTrue();
    assertThat(nack.getAssignmentAck().getGeneration()).isEqualTo(5);
    assertThat(nack.getAssignmentAck().getAccepted()).isFalse();
    assertThat(nack.getAssignmentAck().getErrorMessage())
        .contains("out-of-range endpoint index 3");
    assertThat(assignments).isEmpty();
    assertThat(client.getLatestGeneration()).isEqualTo(0);
  }

  @Test
  public void invalidAssignment_doesNotLeakChunksIntoNextAssignment() throws Exception {
    update(channel, TARGET);
    takeRequest();
    StreamObserver<WatchShardingAssignmentResponse> serverStream = takeServerStream();

    serverStream.onNext(chunkResponse(chunkWithEndpoint("host-a", "", null, 3)));
    serverStream.onNext(metadataResponse(5));
    takeRequest(); // NACK

    serverStream.onNext(chunkResponse(chunkWithEndpoint("host-b", "", null, 0)));
    serverStream.onNext(metadataResponse(6));

    Assignment assignment = takeAssignment();
    assertThat(assignment.getEndpointNames()).containsExactly("host-b");
  }

  @Test
  public void staleGeneration_droppedWithoutAck() throws Exception {
    update(channel, TARGET);
    takeRequest();
    StreamObserver<WatchShardingAssignmentResponse> serverStream = takeServerStream();

    serverStream.onNext(chunkResponse(chunkWithEndpoint("host-a", "", null, 0)));
    serverStream.onNext(metadataResponse(5));
    takeAssignment();
    takeRequest(); // ACK for generation 5

    serverStream.onNext(chunkResponse(chunkWithEndpoint("host-b", "", null, 0)));
    serverStream.onNext(metadataResponse(5));

    assertThat(assignments).isEmpty();
    assertThat(service.requests).isEmpty();
    assertThat(client.getLatestGeneration()).isEqualTo(5);
  }

  @Test
  public void olderGeneration_droppedWithoutAck() throws Exception {
    update(channel, TARGET);
    takeRequest();
    StreamObserver<WatchShardingAssignmentResponse> serverStream = takeServerStream();

    serverStream.onNext(chunkResponse(chunkWithEndpoint("host-a", "", null, 0)));
    serverStream.onNext(metadataResponse(5));
    takeAssignment();
    takeRequest();

    serverStream.onNext(chunkResponse(chunkWithEndpoint("host-b", "", null, 0)));
    serverStream.onNext(metadataResponse(4));

    assertThat(assignments).isEmpty();
    assertThat(service.requests).isEmpty();
  }

  @Test
  public void loadReportingConfig_ignored() throws Exception {
    update(channel, TARGET);
    takeRequest();
    StreamObserver<WatchShardingAssignmentResponse> serverStream = takeServerStream();

    serverStream.onNext(
        WatchShardingAssignmentResponse.newBuilder()
            .setConfig(LoadReportingConfig.newBuilder().setLoadQuantumFraction(0.5))
            .build());

    assertThat(assignments).isEmpty();
    assertThat(service.requests).isEmpty();
  }

  @Test
  public void streamFailure_reconnectsAndSendsLatestGeneration() throws Exception {
    update(channel, TARGET);
    takeRequest();
    StreamObserver<WatchShardingAssignmentResponse> serverStream = takeServerStream();

    serverStream.onNext(chunkResponse(chunkWithEndpoint("host-a", "", null, 0)));
    serverStream.onNext(metadataResponse(9));
    takeAssignment();
    takeRequest(); // ACK

    serverStream.onError(Status.UNAVAILABLE.asRuntimeException());
    fireRetryTimer();

    WatchShardingAssignmentRequest retryRequest = takeRequest();
    assertThat(retryRequest.hasInitialClientConfig()).isTrue();
    assertThat(retryRequest.getInitialClientConfig().getLatestGeneration()).isEqualTo(9);
    assertThat(retryRequest.getInitialClientConfig().getClientUuid()).isEqualTo(CLIENT_UUID);
    assertThat(service.streamCount.get()).isEqualTo(2);
  }

  @Test
  public void streamFailure_doesNotReconnectBeforeBackoffElapses() throws Exception {
    update(channel, TARGET);
    takeRequest();

    takeServerStream().onError(Status.UNAVAILABLE.asRuntimeException());

    assertThat(fakeClock.numPendingTasks()).isEqualTo(1);
    fakeClock.forwardNanos(BACKOFF_NANOS - 1);
    assertThat(service.streamCount.get()).isEqualTo(1);

    fakeClock.forwardNanos(1);
    takeRequest();
    assertThat(service.streamCount.get()).isEqualTo(2);
  }

  @Test
  public void streamCompletedByServer_reconnects() throws Exception {
    update(channel, TARGET);
    takeRequest();
    StreamObserver<WatchShardingAssignmentResponse> serverStream = takeServerStream();

    serverStream.onCompleted();
    fireRetryTimer();

    takeRequest();
    assertThat(service.streamCount.get()).isEqualTo(2);
  }

  @Test
  public void backoffSequence_onlyResetAfterGoodAssignment() throws Exception {
    update(channel, TARGET);
    takeRequest();

    // First failure with no assignment received: a backoff sequence is created.
    takeServerStream().onError(Status.UNAVAILABLE.asRuntimeException());
    fireRetryTimer();
    takeRequest();
    assertThat(backoffPolicyProvider.timesCalled).isEqualTo(1);

    // Second failure with no assignment received: the existing sequence continues.
    takeServerStream().onError(Status.UNAVAILABLE.asRuntimeException());
    fireRetryTimer();
    takeRequest();
    assertThat(backoffPolicyProvider.timesCalled).isEqualTo(1);

    // A good assignment resets the sequence when the stream later fails.
    StreamObserver<WatchShardingAssignmentResponse> serverStream = takeServerStream();
    serverStream.onNext(chunkResponse(chunkWithEndpoint("host-a", "", null, 0)));
    serverStream.onNext(metadataResponse(1));
    takeAssignment();
    takeRequest(); // ACK
    serverStream.onError(Status.UNAVAILABLE.asRuntimeException());
    fireRetryTimer();
    takeRequest();
    assertThat(backoffPolicyProvider.timesCalled).isEqualTo(2);
  }

  @Test
  public void targetChange_restartsStreamAndResetsGeneration() throws Exception {
    update(channel, TARGET);
    takeRequest();
    StreamObserver<WatchShardingAssignmentResponse> serverStream = takeServerStream();
    serverStream.onNext(chunkResponse(chunkWithEndpoint("host-a", "", null, 0)));
    serverStream.onNext(metadataResponse(9));
    takeAssignment();
    takeRequest(); // ACK
    assertThat(client.getLatestGeneration()).isEqualTo(9);

    update(channel, OTHER_TARGET);

    WatchShardingAssignmentRequest request = takeRequest();
    assertThat(request.hasInitialClientConfig()).isTrue();
    assertThat(request.getInitialClientConfig().getTarget()).isEqualTo(OTHER_TARGET);
    assertThat(request.getInitialClientConfig().getLatestGeneration()).isEqualTo(0);
    assertThat(client.getLatestGeneration()).isEqualTo(0);
    assertThat(service.streamCount.get()).isEqualTo(2);
  }

  @Test
  public void channelChange_restartsStreamAndResetsGeneration() throws Exception {
    update(channel, TARGET);
    takeRequest();
    StreamObserver<WatchShardingAssignmentResponse> serverStream = takeServerStream();
    serverStream.onNext(chunkResponse(chunkWithEndpoint("host-a", "", null, 0)));
    serverStream.onNext(metadataResponse(9));
    takeAssignment();
    takeRequest(); // ACK

    String otherServerName = InProcessServerBuilder.generateName();
    grpcCleanup.register(
        InProcessServerBuilder.forName(otherServerName)
            .directExecutor()
            .addService(service)
            .build()
            .start());
    Channel otherChannel =
        grpcCleanup.register(
            InProcessChannelBuilder.forName(otherServerName).directExecutor().build());

    update(otherChannel, TARGET);

    WatchShardingAssignmentRequest request = takeRequest();
    assertThat(request.getInitialClientConfig().getLatestGeneration()).isEqualTo(0);
    assertThat(service.streamCount.get()).isEqualTo(2);
  }

  @Test
  public void shutdown_cancelsStreamAndStopsReconnecting() throws Exception {
    update(channel, TARGET);
    takeRequest();

    syncContext.execute(client::shutdown);

    assertThat(service.streamCount.get()).isEqualTo(1);
    assertThat(fakeClock.numPendingTasks()).isEqualTo(0);
  }

  @Test
  public void shutdown_isIdempotentAndIgnoresLaterUpdates() throws Exception {
    update(channel, TARGET);
    takeRequest();

    syncContext.execute(client::shutdown);
    syncContext.execute(client::shutdown);
    update(channel, OTHER_TARGET);

    assertThat(service.streamCount.get()).isEqualTo(1);
  }

  private void update(Channel channel, String target) {
    syncContext.execute(() -> client.update(channel, target));
  }

  /** Asserts that a retry was scheduled and advances the clock so that it runs. */
  private void fireRetryTimer() {
    assertThat(fakeClock.numPendingTasks()).isEqualTo(1);
    fakeClock.forwardNanos(BACKOFF_NANOS);
  }

  private WatchShardingAssignmentRequest takeRequest() throws Exception {
    WatchShardingAssignmentRequest request =
        service.requests.poll(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    if (request == null) {
      fail("timed out waiting for a request from the autosharding client");
    }
    return request;
  }

  private StreamObserver<WatchShardingAssignmentResponse> takeServerStream() throws Exception {
    StreamObserver<WatchShardingAssignmentResponse> stream =
        service.serverStreams.poll(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    if (stream == null) {
      fail("timed out waiting for the autosharding client to open a stream");
    }
    return stream;
  }

  private Assignment takeAssignment() throws Exception {
    Assignment assignment = assignments.poll(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    if (assignment == null) {
      fail("timed out waiting for an assignment");
    }
    return assignment;
  }

  private static WatchShardingAssignmentResponse chunkResponse(AssignmentChunk chunk) {
    return WatchShardingAssignmentResponse.newBuilder().setChunk(chunk).build();
  }

  private static WatchShardingAssignmentResponse metadataResponse(long generation) {
    return WatchShardingAssignmentResponse.newBuilder()
        .setMetadata(AssignmentMetadata.newBuilder().setGeneration(generation))
        .build();
  }

  private static AssignmentChunk chunkWithEndpoint(
      String endpointName, String startKey, @Nullable String endKey, int endpointIndex) {
    return AssignmentChunk.newBuilder()
        .addEndpoints(endpoint(endpointName))
        .addSliceAssignments(sliceAssignment(startKey, endKey, endpointIndex))
        .build();
  }

  private static EndpointState endpoint(String name) {
    return EndpointState.newBuilder().setEndpoint(name).build();
  }

  private static SliceAssignment sliceAssignment(
      String startKey, @Nullable String endKey, int... endpointIndices) {
    com.google.cloud.autosharding.v1.Slice.Builder slice =
        com.google.cloud.autosharding.v1.Slice.newBuilder()
            .setStartKey(ByteString.copyFromUtf8(startKey));
    if (endKey != null) {
      slice.setEndKey(ByteString.copyFromUtf8(endKey));
    }
    SliceAssignment.Builder builder = SliceAssignment.newBuilder().setSlice(slice);
    for (int index : endpointIndices) {
      builder.addEndpoints(PerSliceEndpointState.newBuilder().setEndpointIndex(index));
    }
    return builder.build();
  }

  private static final class FakeAutoshardingService
      extends AutoshardingServiceGrpc.AutoshardingServiceImplBase {
    final BlockingQueue<WatchShardingAssignmentRequest> requests = new LinkedBlockingQueue<>();
    final BlockingQueue<StreamObserver<WatchShardingAssignmentResponse>> serverStreams =
        new LinkedBlockingQueue<>();
    final AtomicInteger streamCount = new AtomicInteger();

    @Override
    public StreamObserver<WatchShardingAssignmentRequest> watchShardingAssignment(
        StreamObserver<WatchShardingAssignmentResponse> responseObserver) {
      streamCount.incrementAndGet();
      serverStreams.add(responseObserver);
      return new StreamObserver<WatchShardingAssignmentRequest>() {
        @Override
        public void onNext(WatchShardingAssignmentRequest request) {
          requests.add(request);
        }

        @Override
        public void onError(Throwable t) {}

        @Override
        public void onCompleted() {}
      };
    }
  }

  /**
   * Hands out backoff policies with a fixed, non-zero delay so that retries are driven explicitly
   * by the fake clock. The number of policies handed out reflects how many times the backoff
   * sequence was reset.
   */
  private static final class RecordingBackoffPolicyProvider implements BackoffPolicy.Provider {
    int timesCalled;

    @Override
    public BackoffPolicy get() {
      timesCalled++;
      return () -> BACKOFF_NANOS;
    }
  }
}
