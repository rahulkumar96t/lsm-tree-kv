package io.github.rahulkumar96t.lsm.compaction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.rahulkumar96t.lsm.compaction.SizeTieredCompactionPolicy.Run;
import org.junit.jupiter.api.Test;

class SizeTieredCompactionPolicyTest {

  private final SizeTieredCompactionPolicy policy =
      new SizeTieredCompactionPolicy(4, 6, 0.5, 1.5, 10);

  @Test
  void needsMinThresholdSimilarTables() {
    assertThat(policy.pick(new long[] {100, 100, 100})).isEmpty();
    assertThat(policy.pick(new long[] {100, 100, 100, 100})).contains(new Run(0, 4));
  }

  @Test
  void onlyGroupsAdjacentTables() {
    // Four tables of ~100 bytes exist, but a 1000-byte table separates them.
    assertThat(policy.pick(new long[] {100, 100, 1000, 100, 100})).isEmpty();
  }

  @Test
  void prefersTheCheapestTier() {
    long[] sizes = {1000, 1100, 900, 1000, 100, 110, 90, 100};
    assertThat(policy.pick(sizes)).contains(new Run(4, 8));
  }

  @Test
  void capsRunAtMaxThresholdTakingOldestFirst() {
    long[] sizes = {50, 50, 50, 50, 50, 50, 50, 50};
    assertThat(policy.pick(sizes)).contains(new Run(0, 6));
  }

  @Test
  void treatsSmallTablesAsTheFloorSize() {
    // 1, 3 and 9 bytes differ a lot but are all below the 10-byte floor.
    assertThat(policy.pick(new long[] {1, 3, 9, 2})).contains(new Run(0, 4));
  }

  @Test
  void runningAverageStopsATierAtAnOutlier() {
    assertThat(policy.pick(new long[] {100, 120, 110, 400, 380, 390, 410})).contains(new Run(3, 7));
  }

  @Test
  void validatesParameters() {
    assertThatThrownBy(() -> new SizeTieredCompactionPolicy(1, 4, 0.5, 1.5, 0))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new SizeTieredCompactionPolicy(4, 2, 0.5, 1.5, 0))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new SizeTieredCompactionPolicy(4, 8, 1.5, 1.5, 0))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void fullCompactionPicksEverything() {
    assertThat(SizeTieredCompactionPolicy.all(0)).isEmpty();
    assertThat(SizeTieredCompactionPolicy.all(3)).contains(new Run(0, 3));
  }
}
