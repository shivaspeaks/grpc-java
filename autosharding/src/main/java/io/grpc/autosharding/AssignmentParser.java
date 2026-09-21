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

import com.google.cloud.autosharding.v1.AssignmentChunk;
import com.google.cloud.autosharding.v1.EndpointState;
import com.google.cloud.autosharding.v1.PerSliceEndpointState;
import com.google.cloud.autosharding.v1.SliceAssignment;
import com.google.common.collect.ImmutableList;
import com.google.common.io.BaseEncoding;
import com.google.common.primitives.UnsignedBytes;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Combines the {@link AssignmentChunk} messages of a single logical assignment into a
 * validated, sorted, contiguous and gap-free {@link Assignment}.
 *
 * <p>Validation follows gRFC A119, "Handling assignments from the Autosharding server":
 * <ul>
 *   <li>Every endpoint index referenced by a slice must be valid once the endpoint names from
 *       all chunks are combined in chunk order.</li>
 *   <li>A slice's {@code startKey} must not be greater than its {@code endKey}.</li>
 *   <li>Key ranges must not overlap.</li>
 * </ul>
 *
 * <p>Gaps in the key ranges returned by the server are <em>not</em> validation failures. They are
 * explicitly filled with slices containing no endpoints, so that RPCs matching them either fall
 * back (when fallback is enabled) or fail.
 */
final class AssignmentParser {

  /**
   * Thrown when an assignment received from the autosharding server fails validation. The
   * message is suitable for use as the {@code error_message} of an {@code AssignmentAck}.
   */
  static final class ValidationException extends Exception {
    private static final long serialVersionUID = 0L;

    ValidationException(String message) {
      super(message);
    }
  }

  private static final Comparator<byte[]> UNSIGNED_BYTES_COMPARATOR =
      UnsignedBytes.lexicographicalComparator();
  private static final byte[] EMPTY_BYTES = new byte[0];

  private AssignmentParser() {}

  /**
   * Parses and validates the buffered chunks of a single logical assignment.
   *
   * @param chunks the chunks received since the last {@code AssignmentMetadata}, in the order
   *     they were received
   * @param generation the generation number from the terminating {@code AssignmentMetadata}
   * @return a validated, gap-free {@link Assignment} covering the entire keyspace
   * @throws ValidationException if the assignment is invalid
   */
  static Assignment parse(List<AssignmentChunk> chunks, long generation)
      throws ValidationException {
    checkNotNull(chunks, "chunks");

    ImmutableList<String> endpointNames = combineEndpointNames(chunks);
    List<Assignment.Slice> slices = combineSlices(chunks, endpointNames.size());

    slices.sort(
        (s1, s2) -> UNSIGNED_BYTES_COMPARATOR.compare(s1.getStartKey(), s2.getStartKey()));
    checkNoOverlaps(slices);

    return new Assignment(fillGaps(slices), endpointNames, generation);
  }

  /**
   * Concatenates the endpoint names across all chunks, in chunk order. Slice endpoint indices
   * are defined against this combined list.
   */
  private static ImmutableList<String> combineEndpointNames(List<AssignmentChunk> chunks) {
    ImmutableList.Builder<String> names = ImmutableList.builder();
    for (AssignmentChunk chunk : chunks) {
      for (EndpointState endpointState : chunk.getEndpointsList()) {
        names.add(endpointState.getEndpoint());
      }
    }
    return names.build();
  }

  /**
   * Concatenates the slice assignments across all chunks, validating endpoint indices and key
   * range ordering along the way. Slice assignments may appear in any order across chunks.
   */
  private static List<Assignment.Slice> combineSlices(
      List<AssignmentChunk> chunks, int endpointCount) throws ValidationException {
    List<Assignment.Slice> slices = new ArrayList<>();
    for (AssignmentChunk chunk : chunks) {
      for (SliceAssignment sliceAssignment : chunk.getSliceAssignmentsList()) {
        com.google.cloud.autosharding.v1.Slice slice = sliceAssignment.getSlice();
        byte[] startKey = slice.getStartKey().toByteArray();
        byte[] endKey = slice.hasEndKey() ? slice.getEndKey().toByteArray() : null;

        if (endKey != null && UNSIGNED_BYTES_COMPARATOR.compare(startKey, endKey) > 0) {
          throw new ValidationException(
              String.format(
                  "Slice has start_key %s greater than end_key %s",
                  encode(startKey), encode(endKey)));
        }

        List<Integer> endpoints = new ArrayList<>(sliceAssignment.getEndpointsCount());
        for (PerSliceEndpointState perSlice : sliceAssignment.getEndpointsList()) {
          int index = perSlice.getEndpointIndex();
          if (index < 0 || index >= endpointCount) {
            throw new ValidationException(
                String.format(
                    "Slice starting at %s references out-of-range endpoint index %s;"
                        + " assignment contains %s endpoints",
                    encode(startKey), index, endpointCount));
          }
          endpoints.add(index);
        }
        slices.add(new Assignment.Slice(startKey, endKey, endpoints));
      }
    }
    return slices;
  }

  /**
   * Verifies that no two slices in the sorted list cover the same key.
   */
  private static void checkNoOverlaps(List<Assignment.Slice> sorted) throws ValidationException {
    for (int i = 0; i + 1 < sorted.size(); i++) {
      Assignment.Slice current = sorted.get(i);
      Assignment.Slice next = sorted.get(i + 1);
      if (current.getEndKey() == null) {
        throw new ValidationException(
            String.format(
                "Slice starting at %s extends to the end of the keyspace but overlaps the slice"
                    + " starting at %s",
                encode(current.getStartKey()), encode(next.getStartKey())));
      }
      if (UNSIGNED_BYTES_COMPARATOR.compare(current.getEndKey(), next.getStartKey()) > 0) {
        throw new ValidationException(
            String.format(
                "Slice [%s, %s) overlaps the slice starting at %s",
                encode(current.getStartKey()),
                encode(current.getEndKey()),
                encode(next.getStartKey())));
      }
    }
  }

  /**
   * Returns a contiguous list of slices covering {@code ["", inf)}, inserting endpoint-less
   * slices wherever the sorted input leaves a gap.
   */
  private static List<Assignment.Slice> fillGaps(List<Assignment.Slice> sorted) {
    List<Assignment.Slice> filled = new ArrayList<>(sorted.size() + 1);
    // Exclusive upper bound of the key range covered so far; null once infinity is reached.
    byte[] cursor = EMPTY_BYTES;
    for (Assignment.Slice slice : sorted) {
      if (cursor == null) {
        // Unreachable: checkNoOverlaps() rejects any slice following an infinity-ended slice.
        break;
      }
      if (UNSIGNED_BYTES_COMPARATOR.compare(cursor, slice.getStartKey()) < 0) {
        filled.add(new Assignment.Slice(cursor, slice.getStartKey(), ImmutableList.of()));
      }
      filled.add(slice);
      cursor = slice.getEndKey();
    }
    if (cursor != null) {
      filled.add(new Assignment.Slice(cursor, null, ImmutableList.of()));
    }
    return filled;
  }

  private static String encode(@Nullable byte[] key) {
    return key == null ? "inf" : BaseEncoding.base16().encode(key);
  }
}
