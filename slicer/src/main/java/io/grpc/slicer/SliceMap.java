package io.grpc.slicer;

import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import javax.annotation.Nullable;

final class SliceMap {

  static final class SliceEntry {
    final ByteString startKey;
    final List<Integer> endpoints;

    SliceEntry(ByteString startKey, List<Integer> endpoints) {
      this.startKey = startKey;
      this.endpoints = Collections.unmodifiableList(new ArrayList<>(endpoints));
    }
  }

  private final List<SliceEntry> slices;
  private final List<Integer> fallbackPool;
  private final long generation;

  SliceMap(List<SliceEntry> slices, List<Integer> fallbackPool, long generation) {
    List<SliceEntry> sortedSlices = new ArrayList<>(slices);
    sortedSlices.sort(
        Comparator.comparing(e -> e.startKey, ByteString.unsignedLexicographicalComparator()));
    this.slices = Collections.unmodifiableList(sortedSlices);
    this.fallbackPool = Collections.unmodifiableList(new ArrayList<>(fallbackPool));
    this.generation = generation;
  }

  /**
   * Looks up the matching slice index for the given key.
   * Returns null if slices is empty (e.g. startup/fallback case where there are no assignments).
   */
  @Nullable
  Integer lookup(ByteString key) {
    if (slices.isEmpty()) {
      return null;
    }
    int idx = Collections.binarySearch(slices, new SliceEntry(key, Collections.emptyList()), 
        Comparator.comparing(e -> e.startKey, ByteString.unsignedLexicographicalComparator()));

    if (idx >= 0) {
      // Exact match on startKey
      return idx;
    } else {
      // Insertion point (first slice where start_key > key)
      int insertionPoint = -idx - 1;
      if (insertionPoint == 0) {
        // Key is smaller than first slice's startKey
        return null;
      }
      return insertionPoint - 1;
    }
  }

  List<SliceEntry> getSlices() {
    return slices;
  }

  List<Integer> getFallbackPool() {
    return fallbackPool;
  }

  long getGeneration() {
    return generation;
  }
}
