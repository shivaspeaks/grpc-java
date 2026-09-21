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
import static org.junit.Assert.assertThrows;

import com.google.cloud.autosharding.v1.AssignmentChunk;
import com.google.cloud.autosharding.v1.EndpointState;
import com.google.cloud.autosharding.v1.PerSliceEndpointState;
import com.google.cloud.autosharding.v1.SliceAssignment;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link AssignmentParser}. */
@RunWith(JUnit4.class)
public class AssignmentParserTest {

  @Test
  public void parse_singleChunkCoveringWholeKeyspace() throws Exception {
    AssignmentChunk chunk =
        AssignmentChunk.newBuilder()
            .addEndpoints(endpoint("host-a"))
            .addEndpoints(endpoint("host-b"))
            .addSliceAssignments(sliceAssignment("", "m", 0))
            .addSliceAssignments(sliceAssignment("m", null, 1))
            .build();

    Assignment assignment = AssignmentParser.parse(ImmutableList.of(chunk), 7);

    assertThat(assignment.getGeneration()).isEqualTo(7);
    assertThat(assignment.getEndpointNames()).containsExactly("host-a", "host-b").inOrder();
    assertThat(assignment.getSlices()).hasSize(2);
    assertSlice(assignment.getSlices().get(0), "", "m", 0);
    assertSlice(assignment.getSlices().get(1), "m", null, 1);
  }

  @Test
  public void parse_endpointNamesCombinedInChunkOrder() throws Exception {
    AssignmentChunk chunk1 =
        AssignmentChunk.newBuilder()
            .addEndpoints(endpoint("host-a"))
            .addEndpoints(endpoint("host-b"))
            .build();
    AssignmentChunk chunk2 =
        AssignmentChunk.newBuilder()
            .addEndpoints(endpoint("host-c"))
            // Index 2 only resolves once chunk1's endpoints are prepended.
            .addSliceAssignments(sliceAssignment("", null, 2))
            .build();

    Assignment assignment = AssignmentParser.parse(ImmutableList.of(chunk1, chunk2), 1);

    assertThat(assignment.getEndpointNames())
        .containsExactly("host-a", "host-b", "host-c")
        .inOrder();
    assertSlice(assignment.getSlices().get(0), "", null, 2);
  }

  @Test
  public void parse_slicesAcrossChunksAreSorted() throws Exception {
    AssignmentChunk chunk1 =
        AssignmentChunk.newBuilder()
            .addEndpoints(endpoint("host-a"))
            .addSliceAssignments(sliceAssignment("m", null, 0))
            .build();
    AssignmentChunk chunk2 =
        AssignmentChunk.newBuilder().addSliceAssignments(sliceAssignment("", "m", 0)).build();

    Assignment assignment = AssignmentParser.parse(ImmutableList.of(chunk1, chunk2), 1);

    assertThat(assignment.getSlices()).hasSize(2);
    assertSlice(assignment.getSlices().get(0), "", "m", 0);
    assertSlice(assignment.getSlices().get(1), "m", null, 0);
  }

  @Test
  public void parse_fillsLeadingGap() throws Exception {
    AssignmentChunk chunk =
        AssignmentChunk.newBuilder()
            .addEndpoints(endpoint("host-a"))
            .addSliceAssignments(sliceAssignment("d", null, 0))
            .build();

    Assignment assignment = AssignmentParser.parse(ImmutableList.of(chunk), 1);

    assertThat(assignment.getSlices()).hasSize(2);
    assertSlice(assignment.getSlices().get(0), "", "d");
    assertSlice(assignment.getSlices().get(1), "d", null, 0);
  }

  @Test
  public void parse_fillsTrailingGap() throws Exception {
    AssignmentChunk chunk =
        AssignmentChunk.newBuilder()
            .addEndpoints(endpoint("host-a"))
            .addSliceAssignments(sliceAssignment("", "d", 0))
            .build();

    Assignment assignment = AssignmentParser.parse(ImmutableList.of(chunk), 1);

    assertThat(assignment.getSlices()).hasSize(2);
    assertSlice(assignment.getSlices().get(0), "", "d", 0);
    assertSlice(assignment.getSlices().get(1), "d", null);
  }

  @Test
  public void parse_fillsInteriorGap() throws Exception {
    AssignmentChunk chunk =
        AssignmentChunk.newBuilder()
            .addEndpoints(endpoint("host-a"))
            .addEndpoints(endpoint("host-b"))
            .addSliceAssignments(sliceAssignment("", "d", 0))
            .addSliceAssignments(sliceAssignment("m", null, 1))
            .build();

    Assignment assignment = AssignmentParser.parse(ImmutableList.of(chunk), 1);

    assertThat(assignment.getSlices()).hasSize(3);
    assertSlice(assignment.getSlices().get(0), "", "d", 0);
    assertSlice(assignment.getSlices().get(1), "d", "m");
    assertSlice(assignment.getSlices().get(2), "m", null, 1);
  }

  @Test
  public void parse_noSlices_yieldsSingleEmptySliceCoveringKeyspace() throws Exception {
    Assignment assignment =
        AssignmentParser.parse(ImmutableList.of(AssignmentChunk.getDefaultInstance()), 3);

    assertThat(assignment.getSlices()).hasSize(1);
    assertSlice(assignment.getSlices().get(0), "", null);
    assertThat(assignment.getEndpointNames()).isEmpty();
    assertThat(assignment.getGeneration()).isEqualTo(3);
  }

  @Test
  public void parse_noChunks_yieldsSingleEmptySliceCoveringKeyspace() throws Exception {
    Assignment assignment = AssignmentParser.parse(ImmutableList.of(), 1);

    assertThat(assignment.getSlices()).hasSize(1);
    assertSlice(assignment.getSlices().get(0), "", null);
  }

  @Test
  public void parse_sliceWithNoEndpoints_isPreserved() throws Exception {
    AssignmentChunk chunk =
        AssignmentChunk.newBuilder()
            .addEndpoints(endpoint("host-a"))
            .addSliceAssignments(sliceAssignment("", "d"))
            .addSliceAssignments(sliceAssignment("d", null, 0))
            .build();

    Assignment assignment = AssignmentParser.parse(ImmutableList.of(chunk), 1);

    assertThat(assignment.getSlices()).hasSize(2);
    assertSlice(assignment.getSlices().get(0), "", "d");
    assertSlice(assignment.getSlices().get(1), "d", null, 0);
  }

  @Test
  public void parse_multipleEndpointsPerSlice() throws Exception {
    AssignmentChunk chunk =
        AssignmentChunk.newBuilder()
            .addEndpoints(endpoint("host-a"))
            .addEndpoints(endpoint("host-b"))
            .addSliceAssignments(sliceAssignment("", null, 0, 1))
            .build();

    Assignment assignment = AssignmentParser.parse(ImmutableList.of(chunk), 1);

    assertSlice(assignment.getSlices().get(0), "", null, 0, 1);
  }

  @Test
  public void parse_endpointIndexOutOfRange_throws() {
    AssignmentChunk chunk =
        AssignmentChunk.newBuilder()
            .addEndpoints(endpoint("host-a"))
            .addSliceAssignments(sliceAssignment("", null, 1))
            .build();

    AssignmentParser.ValidationException e =
        assertThrows(
            AssignmentParser.ValidationException.class,
            () -> AssignmentParser.parse(ImmutableList.of(chunk), 1));
    assertThat(e).hasMessageThat().contains("out-of-range endpoint index 1");
  }

  @Test
  public void parse_negativeEndpointIndex_throws() {
    AssignmentChunk chunk =
        AssignmentChunk.newBuilder()
            .addEndpoints(endpoint("host-a"))
            .addSliceAssignments(sliceAssignment("", null, -1))
            .build();

    AssignmentParser.ValidationException e =
        assertThrows(
            AssignmentParser.ValidationException.class,
            () -> AssignmentParser.parse(ImmutableList.of(chunk), 1));
    assertThat(e).hasMessageThat().contains("out-of-range endpoint index -1");
  }

  @Test
  public void parse_startKeyGreaterThanEndKey_throws() {
    AssignmentChunk chunk =
        AssignmentChunk.newBuilder().addSliceAssignments(sliceAssignment("z", "a")).build();

    AssignmentParser.ValidationException e =
        assertThrows(
            AssignmentParser.ValidationException.class,
            () -> AssignmentParser.parse(ImmutableList.of(chunk), 1));
    assertThat(e).hasMessageThat().contains("greater than end_key");
  }

  @Test
  public void parse_overlappingSlices_throws() {
    AssignmentChunk chunk =
        AssignmentChunk.newBuilder()
            .addSliceAssignments(sliceAssignment("a", "m"))
            .addSliceAssignments(sliceAssignment("d", null))
            .build();

    AssignmentParser.ValidationException e =
        assertThrows(
            AssignmentParser.ValidationException.class,
            () -> AssignmentParser.parse(ImmutableList.of(chunk), 1));
    assertThat(e).hasMessageThat().contains("overlaps");
  }

  @Test
  public void parse_duplicateStartKeys_throws() {
    AssignmentChunk chunk =
        AssignmentChunk.newBuilder()
            .addSliceAssignments(sliceAssignment("a", "m"))
            .addSliceAssignments(sliceAssignment("a", "z"))
            .build();

    AssignmentParser.ValidationException e =
        assertThrows(
            AssignmentParser.ValidationException.class,
            () -> AssignmentParser.parse(ImmutableList.of(chunk), 1));
    assertThat(e).hasMessageThat().contains("overlaps");
  }

  @Test
  public void parse_sliceExtendingToInfinityFollowedByAnother_throws() {
    AssignmentChunk chunk =
        AssignmentChunk.newBuilder()
            .addSliceAssignments(sliceAssignment("a", null))
            .addSliceAssignments(sliceAssignment("m", null))
            .build();

    AssignmentParser.ValidationException e =
        assertThrows(
            AssignmentParser.ValidationException.class,
            () -> AssignmentParser.parse(ImmutableList.of(chunk), 1));
    assertThat(e).hasMessageThat().contains("extends to the end of the keyspace");
  }

  @Test
  public void parse_unsignedByteOrderingIsUsed() throws Exception {
    // 0x80 is negative as a signed byte but must sort after 0x01.
    AssignmentChunk chunk =
        AssignmentChunk.newBuilder()
            .addSliceAssignments(
                SliceAssignment.newBuilder()
                    .setSlice(
                        com.google.cloud.autosharding.v1.Slice.newBuilder()
                            .setStartKey(ByteString.copyFrom(new byte[] {(byte) 0x80}))))
            .addSliceAssignments(
                SliceAssignment.newBuilder()
                    .setSlice(
                        com.google.cloud.autosharding.v1.Slice.newBuilder()
                            .setStartKey(ByteString.copyFrom(new byte[] {0x01}))
                            .setEndKey(ByteString.copyFrom(new byte[] {(byte) 0x80}))))
            .build();

    Assignment assignment = AssignmentParser.parse(ImmutableList.of(chunk), 1);

    // Leading gap ["", 0x01) plus the two declared slices.
    assertThat(assignment.getSlices()).hasSize(3);
    assertThat(assignment.getSlices().get(1).getStartKey()).isEqualTo(new byte[] {0x01});
    assertThat(assignment.getSlices().get(2).getStartKey()).isEqualTo(new byte[] {(byte) 0x80});
    assertThat(assignment.getSlices().get(2).getEndKey()).isNull();
  }

  @Test
  public void parse_resultingSlicesAreContiguous() throws Exception {
    AssignmentChunk chunk =
        AssignmentChunk.newBuilder()
            .addEndpoints(endpoint("host-a"))
            .addSliceAssignments(sliceAssignment("b", "d", 0))
            .addSliceAssignments(sliceAssignment("k", "m", 0))
            .build();

    Assignment assignment = AssignmentParser.parse(ImmutableList.of(chunk), 1);

    List<Assignment.Slice> slices = assignment.getSlices();
    assertThat(slices.get(0).getStartKey()).isEqualTo(new byte[0]);
    for (int i = 0; i + 1 < slices.size(); i++) {
      assertThat(slices.get(i).getEndKey()).isEqualTo(slices.get(i + 1).getStartKey());
    }
    assertThat(slices.get(slices.size() - 1).getEndKey()).isNull();
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

  private static void assertSlice(
      Assignment.Slice slice, String startKey, @Nullable String endKey, int... endpoints) {
    assertThat(slice.getStartKey()).isEqualTo(startKey.getBytes(StandardCharsets.UTF_8));
    if (endKey == null) {
      assertThat(slice.getEndKey()).isNull();
    } else {
      assertThat(slice.getEndKey()).isEqualTo(endKey.getBytes(StandardCharsets.UTF_8));
    }
    assertThat(slice.getEndpoints())
        .containsExactlyElementsIn(Arrays.stream(endpoints).boxed().toArray())
        .inOrder();
  }
}
