package com.wherobots.db.jdbc;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class WherobotsStructResultTest {
    private static final Field DAY = Field.nullable("day", new ArrowType.Date(DateUnit.DAY));
    private static final Field COUNT = Field.nullable("count", new ArrowType.Int(32, true));
    private static final Field LABEL = Field.nullable("label", new ArrowType.Utf8());
    private static final Date EXPECTED_DAY = Date.valueOf(LocalDate.ofEpochDay(20000));

    @Test
    void nullDateDoesNotTurnFollowingIntegerIntoDate() throws Exception {
        assertStruct(List.of(DAY, COUNT), null, 7, null, Map.of("count", 7));
    }

    @Test
    void nullIntegerDoesNotLeaveFollowingDateAsEpochDays() throws Exception {
        assertStruct(List.of(COUNT, DAY), 20000, null, null, Map.of("day", EXPECTED_DAY));
    }

    @Test
    void nullDateDoesNotMakeFollowingStringThrow() throws Exception {
        assertStruct(List.of(DAY, LABEL), null, null, "place", Map.of("label", "place"));
    }

    @Test
    void nullMiddleChildDoesNotShiftLaterTypes() throws Exception {
        assertStruct(List.of(LABEL, COUNT, DAY), 20000, null, "place",
                Map.of("label", "place", "day", EXPECTED_DAY));
    }

    @Test
    void completeAndAllNullStructsKeepTheirExistingShape() throws Exception {
        assertStruct(List.of(DAY, COUNT, LABEL), 20000, 7, "place",
                Map.of("day", EXPECTED_DAY, "count", 7, "label", "place"));
        assertStruct(List.of(DAY, COUNT, LABEL), null, null, null, Map.of());
    }

    @Test
    void nestedStructUsesItsOwnSchemaAfterNullChild() throws Exception {
        Field nested = structField("nested", List.of(COUNT, DAY));
        Field outer = structField("place", List.of(LABEL, nested));
        try (RootAllocator allocator = new RootAllocator();
             VectorSchemaRoot root = VectorSchemaRoot.create(new Schema(List.of(outer)), allocator)) {
            root.allocateNew();
            StructVector place = (StructVector) root.getVector(0);
            place.setIndexDefined(0);
            StructVector child = place.getChild("nested", StructVector.class);
            child.setIndexDefined(0);
            child.getChild("day", DateDayVector.class).setSafe(0, 20000);
            root.setRowCount(1);
            try (WherobotsResultSet result = read(root, allocator)) {
                assertTrue(result.next());
                Map<?, ?> value = (Map<?, ?>) result.getObject("place");
                assertEquals(Map.of("day", EXPECTED_DAY), value.get("nested"));
                assertFalse(value.containsKey("label"));
                assertFalse(result.wasNull());
            }
        }
    }

    // ---- Arrow IPC fixture helpers ----

    private static Field structField(String name, List<Field> children) {
        return new Field(name, FieldType.nullable(new ArrowType.Struct()), children);
    }

    private static void assertStruct(List<Field> children, Integer day, Integer count,
                                     String label, Map<String, ?> expected) throws Exception {
        Field field = structField("place", children);
        try (RootAllocator allocator = new RootAllocator();
             VectorSchemaRoot root = VectorSchemaRoot.create(new Schema(List.of(field)), allocator)) {
            root.allocateNew();
            StructVector struct = (StructVector) root.getVector(0);
            struct.setIndexDefined(0);
            if (day != null) {
                struct.getChild("day", DateDayVector.class).setSafe(0, day);
            }
            if (count != null) {
                struct.getChild("count", IntVector.class).setSafe(0, count);
            }
            if (label != null) {
                struct.getChild("label", VarCharVector.class).setSafe(0, label.getBytes(StandardCharsets.UTF_8));
            }
            // A null parent remains null even when its child vectors contain data.
            struct.setNull(1);
            root.setRowCount(2);
            try (WherobotsResultSet result = read(root, allocator)) {
                assertTrue(result.next());
                Map<?, ?> value = (Map<?, ?>) result.getObject(1);
                assertEquals(expected.size(), value.size());
                for (Map.Entry<String, ?> entry : expected.entrySet()) {
                    Object actual = value.get(entry.getKey());
                    if (actual instanceof org.apache.arrow.vector.util.Text) {
                        actual = actual.toString();
                    }
                    assertEquals(entry.getValue(), actual, entry.getKey());
                }
                assertFalse(result.wasNull());
                assertTrue(result.next());
                assertNull(result.getObject(1));
                assertTrue(result.wasNull());
                assertFalse(result.next());
            }
        }
    }

    /** Keep the proof on the public Arrow reader path without a cloud session. */
    private static WherobotsResultSet read(VectorSchemaRoot root, RootAllocator allocator) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, bytes)) {
            writer.start();
            writer.writeBatch();
            writer.end();
        }
        return new WherobotsResultSet(null,
                new ArrowStreamReader(new ByteArrayInputStream(bytes.toByteArray()), allocator));
    }

    // ---- End Arrow IPC fixture helpers ----
}
