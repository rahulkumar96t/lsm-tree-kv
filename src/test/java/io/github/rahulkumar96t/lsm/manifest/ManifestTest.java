package io.github.rahulkumar96t.lsm.manifest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.rahulkumar96t.lsm.CorruptionException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ManifestTest {

  @TempDir Path dir;

  @Test
  void missingManifestMeansNewDatabase() throws Exception {
    assertThat(Manifest.load(dir)).isEmpty();
  }

  @Test
  void roundTrips() throws Exception {
    Manifest m = new Manifest(17, 1234, List.of(5L, 9L, 12L));
    m.store(dir);
    assertThat(Manifest.load(dir)).contains(m);

    Manifest empty = new Manifest(3, 0, List.of());
    empty.store(dir);
    assertThat(Manifest.load(dir)).contains(empty);
    assertThat(dir.resolve("MANIFEST.tmp")).doesNotExist();
  }

  @Test
  void detectsTampering() throws Exception {
    new Manifest(17, 1234, List.of(5L, 9L)).store(dir);
    Path file = dir.resolve(Manifest.FILE_NAME);
    Files.writeString(file, Files.readString(file).replace("tables 5 9", "tables 5 8"));

    assertThatThrownBy(() -> Manifest.load(dir))
        .isInstanceOf(CorruptionException.class)
        .hasMessageContaining("checksum");
  }
}
