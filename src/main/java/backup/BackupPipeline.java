package backup;

import backup.config.Config;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.joda.time.Duration;

import java.io.IOException;

public class BackupPipeline {

    private interface Options extends DataflowPipelineOptions {
    }

    public static void main(String[] args) throws IOException {
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        Pipeline pipeline = Pipeline.create(options);

        CoderRegistry registry = pipeline.getCoderRegistry();
        registry.registerCoderForClass(TableDestination.class, SerializableCoder.of(TableDestination.class));
        registry.registerCoderForClass(TableRow.class, TableRowJsonCoder.of());

        PCollectionList<TableRow> pubsub = PCollectionList.empty(pipeline);
        for (Config.Subscription subscription : Config.read(options.getProject()).subscriptions) {
            String displayName = subscription.displayName;

            PubsubIO.Read<PubsubMessage> io = PubsubIO.readMessagesWithAttributes()
                    .fromSubscription(subscription.subscriptionName);

            if (subscription.timestampAttribute != null) {
                io = io.withTimestampAttribute(subscription.timestampAttribute);
            }
            if (subscription.idAttribute != null) {
                io = io.withIdAttribute(subscription.idAttribute);
            }

            PCollection<TableRow> pass = pipeline.apply("Read " + displayName, io)
                    .apply("Transform " + displayName, ParDo.of(new PubSub2BackupRowFn(subscription.topicName)));
            pubsub = pubsub.and(pass);
        }

        pubsub.apply(Flatten.<TableRow>pCollections())
                .apply("Fixed Windows",
                        Window.<TableRow>into(FixedWindows.of(Duration.standardMinutes(1))))

                .apply("BQWrite", BigQueryIO.writeTableRows()
                        .to(TableRefPartition.perDay(
                                options.getProject(),
                                "backup",
                                "pubsub_messages"))
                        .withSchema(SchemaFor.backupEvent())
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                );
        pipeline.run();
    }
}
