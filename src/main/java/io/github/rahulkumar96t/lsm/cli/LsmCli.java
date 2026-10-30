package io.github.rahulkumar96t.lsm.cli;

import io.github.rahulkumar96t.lsm.LsmTree;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

/**
 * Interactive shell: {@code java -jar lsm-tree-kv.jar <data-dir>}. Commands can also be piped in on
 * stdin, one per line.
 */
public final class LsmCli {

  private LsmCli() {}

  public static void main(String[] args) throws IOException {
    if (args.length != 1) {
      System.err.println("usage: java -jar lsm-tree-kv.jar <data-dir>");
      System.exit(2);
    }
    PrintStream out = System.out;
    boolean interactive = System.console() != null;
    try (LsmTree db = LsmTree.open(Path.of(args[0]));
        BufferedReader in =
            new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8))) {
      CommandInterpreter interpreter = new CommandInterpreter(db);
      if (interactive) {
        out.println("lsm-tree-kv: opened " + args[0] + " (type help)");
      }
      while (true) {
        if (interactive) {
          out.print("lsm> ");
          out.flush();
        }
        String line = in.readLine();
        if (line == null) {
          break;
        }
        String trimmed = line.strip();
        if (trimmed.equalsIgnoreCase("quit") || trimmed.equalsIgnoreCase("exit")) {
          break;
        }
        String result = interpreter.execute(trimmed);
        if (!result.isEmpty()) {
          out.println(result);
        }
      }
    }
  }
}
