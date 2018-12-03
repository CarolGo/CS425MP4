from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
import argparse
import sys
import json


def top_10(dstream):
    parsed = dstream.map(lambda v: json.loads(v))
    # # parsed.pprint()
    new_result = parsed.map(lambda words: words['asin']).map(lambda line: line.split(" "))
    new_result.pprint()
    elem = new_result.flatMap(lambda word: word)
    elem.pprint()
    count = elem.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b).transform(
        lambda seq: seq.sortBy(lambda x: x[1], ascending=False))
    return count


if __name__ == '__main__':
    # Get input/output files from user
        parser = argparse.ArgumentParser()

        parser.add_argument('--input_stream', help='Stream URL to pull data from')
        parser.add_argument('--input_stream_port', help='Stream port to pull data from')

    # parser.add_argument('output', help='Directory to save DStream results to')
        args = parser.parse_args()

        # Setup Spark
        conf = SparkConf().setAppName("trending_hashtags")
        sc = SparkContext(conf=conf)
        ssc = StreamingContext(sc, 10)
        ssc.checkpoint('streaming_checkpoints')

        if args.input_stream and args.input_stream_port:
            dstream = ssc.socketTextStream(args.input_stream,
                                           int(args.input_stream_port))

            results = top_10(dstream)
        else:
            print("Need input_stream and input_stream_port")
            sys.exit(1)

        # results.saveAsTextFiles(args.output)
        results.pprint()

        ssc.start()
        ssc.awaitTermination()
