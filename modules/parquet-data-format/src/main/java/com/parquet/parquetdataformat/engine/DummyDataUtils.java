package com.parquet.parquetdataformat.engine;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.index.engine.exec.DocumentInput;
import org.opensearch.index.mapper.MappedFieldType;
import com.parquet.parquetdataformat.converter.FieldTypeConverter;

import java.util.Arrays;
import java.util.Random;

@SuppressForbidden(reason = "Need random for creating temp files")
public class DummyDataUtils {
    public static Schema getSchema() {
        // Create the most minimal schema possible - just one string field
        return new Schema(Arrays.asList(
                Field.notNullable(ID, new ArrowType.Int(32, true)),
                Field.nullable(NAME, new ArrowType.Utf8()),
                Field.nullable(DESIGNATION, new ArrowType.Utf8()),
                Field.nullable(SALARY, new ArrowType.Int(32, true))
        ));
    }

    public static void populateDocumentInput(DocumentInput<?> documentInput) {
        MappedFieldType idField = FieldTypeConverter.convertToMappedFieldType(ID, new ArrowType.Int(32, true));
        documentInput.addField(idField, generateRandomId());
        MappedFieldType nameField = FieldTypeConverter.convertToMappedFieldType(NAME, new ArrowType.Utf8());
        documentInput.addField(nameField, generateRandomName());
        MappedFieldType designationField = FieldTypeConverter.convertToMappedFieldType(DESIGNATION, new ArrowType.Utf8());
        documentInput.addField(designationField, generateRandomDesignation());
        MappedFieldType salaryField = FieldTypeConverter.convertToMappedFieldType(SALARY, new ArrowType.Int(32, true));
        documentInput.addField(salaryField, random.nextInt(100000));
    }

    private static final String ID = "id";
    private static final String NAME = "name";
    private static final String DESIGNATION = "designation";
    private static final String SALARY = "salary";
    private static final String INCREMENT = "increment";
    private static final Random random = new Random();
    private static final String[] NAMES = {"John Doe", "Jane Smith", "Alice Johnson", "Bob Wilson", "Carol Brown"};
    private static final String[] DESIGNATIONS = {"Software Engineer", "Senior Developer", "Team Lead", "Manager", "Architect"};

    private static int generateRandomId() {
        return random.nextInt(1000000);
    }

    private static String generateRandomName() {
        return NAMES[random.nextInt(NAMES.length)];
    }

    private static String generateRandomDesignation() {
        return DESIGNATIONS[random.nextInt(DESIGNATIONS.length)];
    }


}
