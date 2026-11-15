// package org.mahindradatahandler.metrics;

// import io.prometheus.metrics.core.metrics.Counter;
// import io.prometheus.metrics.core.metrics.Histogram;

// public class PromMetricsHandler {

//     private static final Counter recordsProcessed = Counter.builder()
//             .name("kinesis_reader_records_processed")
//             .help("Total number of records processed.")
//             .register();

//     private static final Counter filesCreated = Counter.builder()
//             .name("kinesis_reader_files_count")
//             .help("Total number of files created by Kinesis reader.")
//             .register();

//     private static final Counter bytesWritten = Counter.builder()
//             .name("kinesis_reader_bytes_written")
//             .help("Total number of bytes written.")
//             .register();

//     private static final Histogram processingTime = Histogram.builder()
//             .name("kinesis_reader_processing_time")
//             .help("Processing time of records in milliseconds.")
//             .register();

//     public PromMetricsHandler() {
//     }

//     public void incrementRecordsProcessed() {
//         recordsProcessed.inc();
//     }

//     public void incrementFilesCreated() {
//         filesCreated.inc();
//     }

//     public void incrementBytesWritten(long bytes) {
//         bytesWritten.inc(bytes);
//     }

//     public void recordProcessingTime(long time) {
//         processingTime.observe(time);
//     }
// }
