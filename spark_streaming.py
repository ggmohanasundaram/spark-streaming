from datetime import datetime

from pyspark import SparkContext, Row
from pyspark.streaming import StreamingContext
from pyspark.streaming.kinesis import KinesisUtils, InitialPositionInStream

from src.common.logger import get_logger
from src.common.spak_utils import get_spark_conf, get_spark_instance

log = get_logger(__name__)


class SparkStreaming:
    def __init__(self, config, streaming_config, spark):
        self.config = config
        self.app_name = config.app_name
        self.streaming_config = streaming_config
        self.spark = spark
        self.conf = get_spark_conf(self.app_name)

    def create_stream_context(self, fun):
        log.info('Inside create_stream_context')
        ssc = StreamingContext.getOrCreate(self.streaming_config.checkpoint_location,
                                           lambda: self.get_streaming_context(fun))

        return ssc

    def get_streaming_context(self, fun):
        sc = SparkContext(conf=self.conf)
        total_executors = int(sc._conf.get('spark.executor.instances'))
        log.info(f'''Create Stream for {self.streaming_config.stream_name} ''')
        log.info(f'''total_executors{total_executors}''')
        # ssc = StreamingContext(self.spark.sparkContext, self.streaming_config.batch_interval)
        ssc = StreamingContext(sc, self.streaming_config.batch_interval)
        streams = []
        for i in range(total_executors):
            streams.append(KinesisUtils.createStream(
                ssc=ssc, kinesisAppName=self.app_name,
                streamName=self.streaming_config.stream_name,
                endpointUrl="https://kinesis.ap-southeast-2.amazonaws.com",
                regionName='ap-southeast-2', initialPositionInStream=InitialPositionInStream.TRIM_HORIZON,
                checkpointInterval=self.streaming_config.check_point_interval))
        log.info(f'''length of streams {len(streams)} ''')
        data = ssc.union(*streams)
        ssc.checkpoint(self.streaming_config.checkpoint_location)
        data.foreachRDD(lambda rdd: fun(rdd, self.config))

        return ssc
