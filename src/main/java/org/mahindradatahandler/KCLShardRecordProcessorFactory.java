package org.mahindradatahandler;

import org.mahindradatahandler.config.LibConfig;
import in.intellicar.infra.lognmetricswriter.observability.*;
// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;

public class KCLShardRecordProcessorFactory implements ShardRecordProcessorFactory {

    private final LibConfig config;
    // private final OtelMetricsHandler metricsHandler;
    private DataHandler dataHandler;
    private KCLConsumer kclConsumer;
    private Logger log;
    private Metrics metrics;

    public KCLShardRecordProcessorFactory(LibConfig config, DataHandler dataHandler, Logger log, Metrics metrics, KCLConsumer kclConsumer) {
        this.config = config;
        this.dataHandler = dataHandler;
        this.log = log;
        this.metrics = metrics;
        this.kclConsumer = kclConsumer;
        // this.metricsHandler = metricsHandler;
    }

    public void setKCLConsumer(KCLConsumer kclConsumer) {
        this.kclConsumer = kclConsumer;
    }

    @Override
    public ShardRecordProcessor shardRecordProcessor() {
        return new KCLRecordProcessor(this.config, this.dataHandler, this.log, this.metrics, this.kclConsumer);
        // return new KCLRecordProcessor(this.config, this.metricsHandler);
    }
}
