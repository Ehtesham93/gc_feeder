// package org.mahindradatahandler.metrics;

// import io.opentelemetry.api.metrics.LongCounter;
// import io.opentelemetry.api.metrics.Meter;
// import io.opentelemetry.api.GlobalOpenTelemetry;

// public class OtelMetricsHandler {

//     private Meter meter;
//     private LongCounter recordsProcessedCounter;
//     private LongCounter filesCreatedCounter;
//     private LongCounter bytesWrittenCounter;
//     private LongCounter processingTimeCounter;

//     public OtelMetricsHandler() {
//         this.meter = GlobalOpenTelemetry.getMeter("kinesis-reader-lib");

//         this.recordsProcessedCounter = this.meter.counterBuilder("records_processed")
//                 .setDescription("Records processed by the Kinesis Reader Library")
//                 .build();

//         this.filesCreatedCounter = this.meter.counterBuilder("files_created")
//                 .setDescription("Files created by the Kinesis Reader Library")
//                 .build();

//         this.bytesWrittenCounter = this.meter.counterBuilder("bytes_written")
//                 .setDescription("Bytes written by the Kinesis Reader Library")
//                 .build();

//         this.processingTimeCounter = this.meter.counterBuilder("processing_time")
//                 .setDescription("Processing time of records in milliseconds")
//                 .build();
//     }

//     public void incrementRecordsProcessed() {
//         this.recordsProcessedCounter.add(1);
//     }

//     public void incrementFilesCreated() {
//         this.filesCreatedCounter.add(1);
//     }

//     public void incrementBytesWritten(long bytes) {
//         this.bytesWrittenCounter.add(bytes);
//     }

//     public void recordProcessingTime(long time) {
//         this.processingTimeCounter.add(time);
//     }
// }
