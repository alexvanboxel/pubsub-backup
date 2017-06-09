package backup;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.joda.time.DateTime;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.Map;

public class PubSub2BackupRowFn extends DoFn<PubsubMessage, TableRow> {

    private transient static ObjectMapper om;

    static {
        om = new ObjectMapper();
    }

    private String topic;

    public static String forBQ(DateTime dateTime) {
        if (dateTime == null) {
            return null;
        }
        DateTimeFormatter formatter =
                DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS").withZoneUTC();
        return formatter.print(dateTime) + " UTC";
    }


    public PubSub2BackupRowFn(String topic) {
        this.topic = topic;
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
        PubsubMessage pubsubMessage = c.element();

        TableRow tableRow = new TableRow();
        tableRow.set("topic", topic);
        tableRow.set("payload", pubsubMessage.getPayload());
        Map<String, String> attributeMap = pubsubMessage.getAttributeMap();
        if (attributeMap != null) {
            tableRow.set("attributes", om.writeValueAsString(attributeMap));
        }
        tableRow.set("msg_timestamp", forBQ(c.timestamp().toDateTime()));
        tableRow.set("process_timestamp", forBQ(Instant.now().toDateTime()));

        c.output(tableRow);
    }
}