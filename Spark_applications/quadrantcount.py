from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
import argparse
import sys


def get_quadrant(dstream):
    # Convert the input string into a pair of numbers
    try:
        (x, y) = [float(x) for x in dstream.split()]
    except:
        print("Invalid input")
        return 'Invalid points', 1

    # Map the pair of numbers to the right quadrant
    if x > 0 and y > 0:
        quadrant = 'First quadrant'
    elif x < 0 and y > 0:
        quadrant = 'Second quadrant'
    elif x < 0 and y < 0:
        quadrant = 'Third quadrant'
    elif x > 0 and y < 0:
        quadrant = 'Fourth quadrant'
    elif x == 0 and y != 0:
        quadrant = 'Lies on Y axis'
    elif x != 0 and y == 0:
        quadrant = 'Lies on X axis'
    else:
        quadrant = 'Origin'
    # The pair represents the quadrant and the counter increment
    return quadrant, 1


if __name__ == '__main__':
    # Get input/output files from user
    parser = argparse.ArgumentParser()

    parser.add_argument('--input_stream', help='Stream URL to pull data from')
    parser.add_argument('--input_stream_port', help='Stream port to pull data from')

    # parser.add_argument('output', help='Directory to save DStream results to')
    args = parser.parse_args()

    # Setup Spark
    conf = SparkConf().setAppName("get_quadrant")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 1)
    ssc.checkpoint('streaming_checkpoints')

    if args.input_stream and args.input_stream_port:
        dstream = ssc.socketTextStream(args.input_stream,
                                       int(args.input_stream_port))
        # updateFunction = lambda new_values, running_count: sum(new_values) + (running_count or 0)
        running_counts = dstream.map(get_quadrant).updateStateByKey(
            lambda new_values, running_count: sum(new_values) + running_count or 0)
    else:
        print("Need input_stream and input_stream_port")
        sys.exit(1)

    # results.saveAsTextFiles(args.output)
    running_counts.pprint()

    ssc.start()
    ssc.awaitTermination()
