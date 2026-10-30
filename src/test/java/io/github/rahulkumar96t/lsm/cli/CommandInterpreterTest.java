package io.github.rahulkumar96t.lsm.cli;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.rahulkumar96t.lsm.LsmTree;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CommandInterpreterTest {

  @TempDir Path dir;

  @Test
  void executesCommandsAgainstTheStore() throws Exception {
    try (LsmTree db = LsmTree.open(dir)) {
      CommandInterpreter cli = new CommandInterpreter(db);

      assertThat(cli.execute("put user:1 alice")).isEqualTo("OK");
      assertThat(cli.execute("put user:2 \"bob smith\"")).isEqualTo("OK");
      assertThat(cli.execute("put user:3 carol")).isEqualTo("OK");
      assertThat(cli.execute("get user:2")).isEqualTo("bob smith");
      assertThat(cli.execute("del user:1")).isEqualTo("OK");
      assertThat(cli.execute("get user:1")).isEqualTo("(not found)");
      assertThat(cli.execute("flush")).isEqualTo("OK");
      assertThat(cli.execute("scan")).isEqualTo("user:2 = bob smith\nuser:3 = carol\n(2 keys)");
      assertThat(cli.execute("scan - user:3")).isEqualTo("user:2 = bob smith\n(1 keys)");
      assertThat(cli.execute("scan - - 1"))
          .isEqualTo("user:2 = bob smith\n... (limit 1 reached)\n(1 keys)");
      assertThat(cli.execute("compact")).isEqualTo("OK");
      assertThat(cli.execute("stats")).contains("sstables             1");
    }
  }

  @Test
  void reportsUsageErrorsWithoutThrowing() throws Exception {
    try (LsmTree db = LsmTree.open(dir)) {
      CommandInterpreter cli = new CommandInterpreter(db);

      assertThat(cli.execute("")).isEmpty();
      assertThat(cli.execute("put onlykey")).isEqualTo("error: usage: put <key> <value>");
      assertThat(cli.execute("frobnicate")).startsWith("unknown command");
      assertThat(cli.execute("get \"unterminated")).isEqualTo("error: unterminated quote");
      assertThat(cli.execute("scan a b notanumber")).isEqualTo("error: limit must be a number");
      assertThat(cli.execute("help")).contains("put <key> <value>");
    }
  }

  @Test
  void tokenizerHandlesQuotesAndEscapes() {
    assertThat(CommandInterpreter.tokenize("  put   \"a b\"  \"say \\\"hi\\\"\" "))
        .containsExactly("put", "a b", "say \"hi\"");
    assertThat(CommandInterpreter.tokenize("put k \"\"")).containsExactly("put", "k", "");
  }
}
