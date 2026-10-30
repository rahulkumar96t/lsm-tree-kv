package io.github.rahulkumar96t.lsm.cli;

import io.github.rahulkumar96t.lsm.CloseableIterator;
import io.github.rahulkumar96t.lsm.KeyValue;
import io.github.rahulkumar96t.lsm.LsmStats;
import io.github.rahulkumar96t.lsm.LsmTree;
import io.github.rahulkumar96t.lsm.util.Bytes;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.StringJoiner;

/**
 * Parses and executes one REPL command against an {@link LsmTree}. Keys and values are UTF-8
 * strings; wrap them in double quotes to include spaces.
 */
public final class CommandInterpreter {

  static final String HELP =
      """
      commands:
        put <key> <value>        insert or overwrite a key
        get <key>                print the value of a key
        del <key>                delete a key
        scan [from] [to] [limit] list keys in [from, to) (use - for an open bound)
        flush                    write the memtable to an SSTable
        compact                  merge all SSTables into one
        stats                    print engine statistics
        help                     show this help
        quit                     exit""";

  private static final int DEFAULT_SCAN_LIMIT = 100;

  private final LsmTree db;

  public CommandInterpreter(LsmTree db) {
    this.db = db;
  }

  /** Executes {@code line} and returns the text to print. */
  public String execute(String line) {
    try {
      List<String> args = tokenize(line);
      if (args.isEmpty()) {
        return "";
      }
      String command = args.getFirst().toLowerCase(Locale.ROOT);
      return switch (command) {
        case "put" -> {
          requireArgs(args, 3, "put <key> <value>");
          db.put(Bytes.utf8(args.get(1)), Bytes.utf8(args.get(2)));
          yield "OK";
        }
        case "get" -> {
          requireArgs(args, 2, "get <key>");
          yield db.get(Bytes.utf8(args.get(1))).map(Bytes::string).orElse("(not found)");
        }
        case "del", "delete" -> {
          requireArgs(args, 2, "del <key>");
          db.delete(Bytes.utf8(args.get(1)));
          yield "OK";
        }
        case "scan" -> scan(args);
        case "flush" -> {
          db.flush();
          yield "OK";
        }
        case "compact" -> {
          db.compact();
          yield "OK";
        }
        case "stats" -> formatStats(db.stats());
        case "help" -> HELP;
        default -> "unknown command '" + command + "' (try help)";
      };
    } catch (IllegalArgumentException e) {
      return "error: " + e.getMessage();
    }
  }

  private String scan(List<String> args) {
    byte[] from = args.size() > 1 && !args.get(1).equals("-") ? Bytes.utf8(args.get(1)) : null;
    byte[] to = args.size() > 2 && !args.get(2).equals("-") ? Bytes.utf8(args.get(2)) : null;
    int limit = DEFAULT_SCAN_LIMIT;
    if (args.size() > 3) {
      try {
        limit = Integer.parseInt(args.get(3));
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("limit must be a number");
      }
    }
    StringJoiner out = new StringJoiner("\n");
    int shown = 0;
    try (CloseableIterator<KeyValue> it = db.scan(from, to)) {
      while (it.hasNext() && shown < limit) {
        KeyValue kv = it.next();
        out.add(Bytes.string(kv.key()) + " = " + Bytes.string(kv.value()));
        shown++;
      }
      if (it.hasNext()) {
        out.add("... (limit " + limit + " reached)");
      }
    }
    out.add("(" + shown + " keys)");
    return out.toString();
  }

  private static String formatStats(LsmStats s) {
    return String.join(
        "\n",
        "last sequence        " + s.lastSequence(),
        "memtable bytes       " + s.memtableBytes(),
        "frozen memtables     " + s.immutableMemtables(),
        "sstables             " + s.tableCount() + " " + s.tableSizes(),
        "sstable bytes        " + s.tableBytes(),
        "sstable entries      " + s.tableEntries(),
        "flushes              " + s.flushes(),
        "compactions          " + s.compactions(),
        "shadowed dropped     " + s.shadowedVersionsDropped(),
        "tombstones dropped   " + s.tombstonesDropped(),
        "write stalls         " + s.writeStalls(),
        "wal records replayed " + s.walRecordsReplayed());
  }

  private static void requireArgs(List<String> args, int count, String usage) {
    if (args.size() != count) {
      throw new IllegalArgumentException("usage: " + usage);
    }
  }

  /** Splits on whitespace; double quotes group words and {@code \"} escapes a quote. */
  static List<String> tokenize(String line) {
    List<String> tokens = new ArrayList<>();
    StringBuilder current = new StringBuilder();
    boolean inQuotes = false;
    boolean hasToken = false;
    for (int i = 0; i < line.length(); i++) {
      char c = line.charAt(i);
      if (c == '\\' && i + 1 < line.length() && line.charAt(i + 1) == '"') {
        current.append('"');
        hasToken = true;
        i++;
      } else if (c == '"') {
        inQuotes = !inQuotes;
        hasToken = true;
      } else if (Character.isWhitespace(c) && !inQuotes) {
        if (hasToken) {
          tokens.add(current.toString());
          current.setLength(0);
          hasToken = false;
        }
      } else {
        current.append(c);
        hasToken = true;
      }
    }
    if (inQuotes) {
      throw new IllegalArgumentException("unterminated quote");
    }
    if (hasToken) {
      tokens.add(current.toString());
    }
    return tokens;
  }
}
