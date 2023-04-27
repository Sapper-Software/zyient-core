package ai.sapper.cdc.entity.schema;

import java.util.Comparator;

public class SchemaFieldSorter implements Comparator<SchemaField> {
    /**
     * @param field
     * @param t1
     * @return
     */
    @Override
    public int compare(SchemaField field, SchemaField t1) {
        return (field.getPosition() - t1.getPosition());
    }
}
