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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import com.google.cloud.autosharding.v1.AssignmentAck;
import com.google.cloud.autosharding.v1.AssignmentChunk;
import com.google.cloud.autosharding.v1.AutoshardingServiceGrpc;
import com.google.cloud.autosharding.v1.InitialClientConfig;
import com.google.cloud.autosharding.v1.WatchShardingAssignmentRequest;
import com.google.cloud.autosharding.v1.WatchShardingAssignmentResponse;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.base.Supplier;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.Status;
import io.grpc.SynchronizationContext;
import io.grpc.SynchronizationContext.ScheduledHandle;
import io.grpc.internal.BackoffPolicy;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.ClientResponseObserver;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Encapsulates all communication with an external autosharding service over the
 * {@code WatchShardingAssignment} streaming protocol.
 *
 * <p>This component owns the stream lifecycle, buffers and reassembles chunked assignments,
 * validates them, acknowledges them, and hands validated {@link Assignment}s to the parent load
 * balancer. See gRFC A119, "Communicating with the Autosharding service".
 *
 * <p>Threading model: This class is not thread-safe. All public methods must be invoked from the
 * {@link SynchronizationContext} supplied at construction, and all callbacks to the
 * {@link AssignmentWatcher} are delivered on that same context.
 */
@NotThreadSafe
final class AutoshardingClient {
  private static final Logger logger = Logger.getLogger(AutoshardingClient.class.getName());

  /**
   * Receives validated assignments from the autosharding service.
   */
  interface AssignmentWatcher {
    /**
     * Called with a newly accepted assignment. Invoked on the {@link SynchronizationContext}.
     */
    void onAssignment(Assignment assignment);
  }

  private final SynchronizationContext syncContext;
  private final ScheduledExecutorService timerService;
  private final BackoffPolicy.Provider backoffPolicyProvider;
  private final Stopwatch retryStopwatch;
  private final AssignmentWatcher watcher;
  private final String clientUuid;

  @Nullable private Channel channel;
  @Nullable private String target;

  /**
   * Generation of the most recent accepted assignment. Sent to the server so that it can skip
   * resending an assignment the client already has. Reset to zero whenever the channel or the
   * target changes, because the stored value is meaningless against a different sharding server
   * or a different resource.
   */
  private long latestGeneration;

  @Nullable private BackoffPolicy retryBackoffPolicy;
  @Nullable private ScheduledHandle retryTimer;
  @Nullable private AutoshardingStream stream;
  private boolean shutdown;

  /**
   * Constructs an {@link AutoshardingClient}. No stream is created until
   * {@link #update(Channel, String)} supplies a channel and a target.
   *
   * @param clientUuid a UUID generated once by the parent load balancer and reused across all
   *     stream restarts
   * @param syncContext the context on which all state is mutated and callbacks are delivered
   * @param timerService used to schedule stream retries
   * @param backoffPolicyProvider supplies the exponential backoff sequence for stream retries
   * @param stopwatchSupplier supplies the stopwatch measuring time spent in a stream attempt
   * @param watcher receives validated assignments
   */
  AutoshardingClient(
      String clientUuid,
      SynchronizationContext syncContext,
      ScheduledExecutorService timerService,
      BackoffPolicy.Provider backoffPolicyProvider,
      Supplier<Stopwatch> stopwatchSupplier,
      AssignmentWatcher watcher) {
    this.clientUuid = checkNotNull(clientUuid, "clientUuid");
    this.syncContext = checkNotNull(syncContext, "syncContext");
    this.timerService = checkNotNull(timerService, "timerService");
    this.backoffPolicyProvider = checkNotNull(backoffPolicyProvider, "backoffPolicyProvider");
    this.retryStopwatch = checkNotNull(stopwatchSupplier, "stopwatchSupplier").get();
    this.watcher = checkNotNull(watcher, "watcher");
  }

  /**
   * Applies a new channel and/or resolved autosharding target.
   *
   * <p>If either changed, any existing stream is torn down, the stored generation number is
   * discarded, and a new stream is started immediately. A stored generation number is only
   * meaningful for the combination of sharding server and target that produced it; retaining it
   * across a change could cause the server to withhold assignments indefinitely.
   *
   * @param channel the channel to the sharding service, created via the "Channel Factory"
   * @param target the autosharding target, with any {@code %s} token already substituted
   */
  void update(Channel channel, String target) {
    syncContext.throwIfNotInThisSynchronizationContext();
    checkNotNull(channel, "channel");
    checkNotNull(target, "target");
    if (shutdown) {
      return;
    }
    if (channel.equals(this.channel) && target.equals(this.target)) {
      return;
    }
    this.channel = channel;
    this.target = target;
    this.latestGeneration = 0;
    this.retryBackoffPolicy = null;
    restartStream();
  }

  /**
   * Cancels any in-flight stream and pending retry. The channel is not shut down, because it is
   * owned by the parent load balancer.
   */
  void shutdown() {
    syncContext.throwIfNotInThisSynchronizationContext();
    if (shutdown) {
      return;
    }
    shutdown = true;
    cancelRetryTimer();
    if (stream != null) {
      stream.close(Status.CANCELLED.withDescription("AutoshardingClient shutdown"));
      stream = null;
    }
  }

  @VisibleForTesting
  long getLatestGeneration() {
    return latestGeneration;
  }

  private void restartStream() {
    cancelRetryTimer();
    if (stream != null) {
      stream.close(Status.CANCELLED.withDescription("stream restarted"));
      stream = null;
    }
    startStream();
  }

  private void startStream() {
    if (shutdown || channel == null || target == null) {
      return;
    }
    checkState(stream == null, "previous stream has not been cleared yet");
    retryStopwatch.reset().start();
    stream = new AutoshardingStream();
    stream.start();
  }

  private void cancelRetryTimer() {
    if (retryTimer != null) {
      if (retryTimer.isPending()) {
        retryTimer.cancel();
      }
      retryTimer = null;
    }
  }

  /**
   * Schedules the next stream attempt. Per gRFC A119, backoff only applies to streams that closed
   * without delivering a good logical assignment; the backoff sequence is reset as soon as one is
   * received.
   */
  private void scheduleRetry(boolean receivedGoodAssignment) {
    if (shutdown) {
      return;
    }
    if (receivedGoodAssignment || retryBackoffPolicy == null) {
      retryBackoffPolicy = backoffPolicyProvider.get();
    }
    // The backoff sequence bounds the interval between consecutive stream starts, so the actual
    // delay is reduced by however long the previous attempt lasted. The retry always goes through
    // the timer service, even when no delay remains, so that a channel failing calls synchronously
    // cannot drive unbounded recursion between startStream() and handleStreamClosed().
    long delayNanos =
        Math.max(
            0,
            retryBackoffPolicy.nextBackoffNanos() - retryStopwatch.elapsed(TimeUnit.NANOSECONDS));
    retryTimer =
        syncContext.schedule(this::startStream, delayNanos, TimeUnit.NANOSECONDS, timerService);
  }

  /**
   * A single {@code WatchShardingAssignment} stream.
   */
  private final class AutoshardingStream
      implements ClientResponseObserver<
          WatchShardingAssignmentRequest, WatchShardingAssignmentResponse> {

    /**
     * Chunks received since the last {@code AssignmentMetadata}. A chunk's slices reference
     * endpoint indices into the list combined across all chunks, so chunks cannot be used until
     * the assignment is terminated by an {@code AssignmentMetadata} message.
     */
    private final List<AssignmentChunk> bufferedChunks = new ArrayList<>();

    @Nullable private ClientCallStreamObserver<WatchShardingAssignmentRequest> requestStream;
    private boolean receivedGoodAssignment;
    private boolean closed;

    @Override
    public void beforeStart(
        ClientCallStreamObserver<WatchShardingAssignmentRequest> requestStream) {
      this.requestStream = requestStream;
    }

    void start() {
      // wait_for_ready keeps the stream pending through transient connectivity failures instead
      // of failing it, which recovers faster than applying backoff around stream creation.
      ClientCalls.asyncBidiStreamingCall(
          channel.newCall(
              AutoshardingServiceGrpc.getWatchShardingAssignmentMethod(),
              CallOptions.DEFAULT.withWaitForReady()),
          this);
      sendInitialClientConfig();
    }

    private void sendInitialClientConfig() {
      WatchShardingAssignmentRequest request =
          WatchShardingAssignmentRequest.newBuilder()
              .setInitialClientConfig(
                  InitialClientConfig.newBuilder()
                      .setTarget(target)
                      .setClientUuid(clientUuid)
                      .setLatestGeneration(latestGeneration))
              .build();
      requestStream.onNext(request);
    }

    @Override
    public void onNext(WatchShardingAssignmentResponse response) {
      syncContext.execute(() -> handleResponse(response));
    }

    @Override
    public void onError(Throwable t) {
      syncContext.execute(() -> handleStreamClosed(Status.fromThrowable(t)));
    }

    @Override
    public void onCompleted() {
      syncContext.execute(
          () ->
              handleStreamClosed(
                  Status.UNAVAILABLE.withDescription("autosharding stream closed by server")));
    }

    private void handleResponse(WatchShardingAssignmentResponse response) {
      if (closed) {
        return;
      }
      if (response.hasChunk()) {
        bufferedChunks.add(response.getChunk());
      } else if (response.hasMetadata()) {
        handleAssignmentComplete(response.getMetadata().getGeneration());
      }
      // LoadReportingConfig is intentionally ignored; load reporting is not yet supported.
    }

    /**
     * Reassembles, validates and acknowledges the buffered chunks terminated by an
     * {@code AssignmentMetadata} message.
     */
    private void handleAssignmentComplete(long generation) {
      List<AssignmentChunk> chunks = new ArrayList<>(bufferedChunks);
      bufferedChunks.clear();

      // Generations are monotonically increasing. Anything we have already seen is stale, and is
      // dropped without acknowledgement.
      if (generation <= latestGeneration) {
        logger.log(
            Level.FINE,
            "Dropping autosharding assignment with stale generation {0}; latest is {1}",
            new Object[] {generation, latestGeneration});
        return;
      }

      Assignment assignment;
      try {
        assignment = AssignmentParser.parse(chunks, generation);
      } catch (AssignmentParser.ValidationException e) {
        logger.log(
            Level.WARNING,
            "Rejecting autosharding assignment with generation {0}: {1}",
            new Object[] {generation, e.getMessage()});
        sendAck(generation, false, e.getMessage());
        return;
      }

      sendAck(generation, true, null);
      latestGeneration = generation;
      if (!receivedGoodAssignment) {
        receivedGoodAssignment = true;
      }
      watcher.onAssignment(assignment);
    }

    private void sendAck(long generation, boolean accepted, @Nullable String errorMessage) {
      AssignmentAck.Builder ack =
          AssignmentAck.newBuilder().setGeneration(generation).setAccepted(accepted);
      if (errorMessage != null) {
        ack.setErrorMessage(truncateErrorMessage(errorMessage));
      }
      requestStream.onNext(
          WatchShardingAssignmentRequest.newBuilder().setAssignmentAck(ack).build());
    }

    private void handleStreamClosed(Status status) {
      if (closed) {
        return;
      }
      closed = true;
      logger.log(
          Level.FINE,
          "Autosharding stream closed with status {0}: {1}",
          new Object[] {status.getCode(), status.getDescription()});
      bufferedChunks.clear();
      if (stream == this) {
        stream = null;
        scheduleRetry(receivedGoodAssignment);
      }
    }

    /**
     * Cancels the stream without scheduling a retry. Used when the client is shutting down or
     * when the configuration changed and a fresh stream is being created.
     */
    void close(Status status) {
      if (closed) {
        return;
      }
      closed = true;
      bufferedChunks.clear();
      if (requestStream != null) {
        requestStream.cancel(status.getDescription(), status.getCause());
      }
    }
  }

  /**
   * The {@code error_message} field must not exceed 512 characters.
   */
  private static String truncateErrorMessage(String message) {
    return message.length() <= 512 ? message : message.substring(0, 512);
  }
}
