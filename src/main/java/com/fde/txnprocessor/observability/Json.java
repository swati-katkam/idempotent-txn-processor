package com.fde.txnprocessor.observability;

import java.lang.reflect.RecordComponent;
import java.math.BigDecimal;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/** Minimal pretty-printing JSON writer for the processing report, to avoid a library dependency. */
final class Json {

    private Json() {
    }

    static String write(Object value) {
        StringBuilder out = new StringBuilder();
        write(out, value, 0);
        return out.append('\n').toString();
    }

    private static void write(StringBuilder out, Object value, int depth) {
        if (value == null) {
            out.append("null");
        } else if (value instanceof BigDecimal decimal) {
            out.append(decimal.toPlainString());
        } else if (value instanceof Number || value instanceof Boolean) {
            out.append(value);
        } else if (value instanceof Map<?, ?> map) {
            writeObject(out, map, depth);
        } else if (value instanceof Collection<?> collection) {
            writeArray(out, collection, depth);
        } else if (value instanceof Record record) {
            writeObject(out, recordToMap(record), depth);
        } else {
            quote(out, value.toString());
        }
    }

    private static void writeObject(StringBuilder out, Map<?, ?> map, int depth) {
        if (map.isEmpty()) {
            out.append("{}");
            return;
        }
        out.append("{\n");
        Iterator<? extends Map.Entry<?, ?>> it = map.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<?, ?> entry = it.next();
            indent(out, depth + 1);
            quote(out, String.valueOf(entry.getKey()));
            out.append(": ");
            write(out, entry.getValue(), depth + 1);
            out.append(it.hasNext() ? ",\n" : "\n");
        }
        indent(out, depth);
        out.append('}');
    }

    private static void writeArray(StringBuilder out, Collection<?> collection, int depth) {
        if (collection.isEmpty()) {
            out.append("[]");
            return;
        }
        out.append("[\n");
        Iterator<?> it = collection.iterator();
        while (it.hasNext()) {
            indent(out, depth + 1);
            write(out, it.next(), depth + 1);
            out.append(it.hasNext() ? ",\n" : "\n");
        }
        indent(out, depth);
        out.append(']');
    }

    private static Map<String, Object> recordToMap(Record record) {
        Map<String, Object> fields = new LinkedHashMap<>();
        for (RecordComponent component : record.getClass().getRecordComponents()) {
            try {
                fields.put(component.getName(), component.getAccessor().invoke(record));
            } catch (ReflectiveOperationException e) {
                throw new IllegalStateException("Cannot serialise " + record.getClass().getSimpleName(), e);
            }
        }
        return fields;
    }

    private static void quote(StringBuilder out, String text) {
        out.append('"');
        for (char c : text.toCharArray()) {
            switch (c) {
                case '"' -> out.append("\\\"");
                case '\\' -> out.append("\\\\");
                case '\n' -> out.append("\\n");
                case '\r' -> out.append("\\r");
                case '\t' -> out.append("\\t");
                default -> {
                    if (c < 0x20) {
                        out.append(String.format("\\u%04x", (int) c));
                    } else {
                        out.append(c);
                    }
                }
            }
        }
        out.append('"');
    }

    private static void indent(StringBuilder out, int depth) {
        out.append("  ".repeat(depth));
    }
}
