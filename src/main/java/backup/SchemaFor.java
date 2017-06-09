package backup;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;

import java.util.ArrayList;
import java.util.List;

public class SchemaFor {

    public static TableSchema backupEvent() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("topic").setType("STRING").setMode("REQUIRED"));
        fields.add(new TableFieldSchema().setName("payload").setType("BYTES").setMode("REQUIRED"));
        fields.add(new TableFieldSchema().setName("attributes").setType("STRING").setMode("NULLABLE"));
        fields.add(new TableFieldSchema().setName("msg_id").setType("STRING").setMode("NULLABLE"));
        fields.add(new TableFieldSchema().setName("msg_timestamp").setType("TIMESTAMP").setMode("REQUIRED"));
        fields.add(new TableFieldSchema().setName("process_timestamp").setType("TIMESTAMP").setMode("REQUIRED"));
        fields.add(new TableFieldSchema().setName("errors").setType("STRING").setMode("NULLABLE"));
        TableSchema schema = new TableSchema().setFields(fields);
        return schema;
    }

}
