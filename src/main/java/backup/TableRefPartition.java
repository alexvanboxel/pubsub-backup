package backup;

import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;


public class TableRefPartition implements SerializableFunction<ValueInSingleWindow<TableRow>, TableDestination> {

    private final String projectId;
    private final String datasetId;
    private final String pattern;
    private final String table;

    public static TableRefPartition perDay(String projectId, String datasetId, String tablePrefix) {
        return new TableRefPartition(projectId, datasetId, "yyyyMMdd", tablePrefix + "$");
    }

    private TableRefPartition(String projectId, String datasetId, String pattern, String table) {
        this.projectId = projectId;
        this.datasetId = datasetId;
        this.pattern = pattern;
        this.table = table;
    }

    @Override
    public TableDestination apply(ValueInSingleWindow<TableRow> input) {
        DateTimeFormatter partition = DateTimeFormat.forPattern(pattern).withZoneUTC();

        TableReference reference = new TableReference();
        reference.setProjectId(this.projectId);
        reference.setDatasetId(this.datasetId);

        reference.setTableId(table + input.getWindow().maxTimestamp().toString(partition));
        return new TableDestination(reference, null);
    }
}
