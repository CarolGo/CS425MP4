from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
import argparse
import sys
import json


def check(word):
    word_want = ['i', 'you', 'he', 'she', 'is', 'are', 'toy']
    # word_want = ['i', 'I',
    #              'you', 'You', 'YOU', 'yOu', 'yoU', 'YOu', 'yOU',
    #              'he', 'He', 'HE', 'hE',
    #              'she', 'She', 'sHe', 'shE', 'SHe', 'sHE',
    #              'is', 'Is', 'iS', 'IS',
    #              'are', 'Are', 'aRe', 'arE', 'ARe', 'aRE',
    #              'toy', 'TOY', 'Toy', 'tOy', 'toY', 'TOy', 'tOY']
    # right_word = map(lambda words: words.lower(), word)
    # print(right_word)
    # for right_word in word:
    if word.lower() in word_want:
        return True
    else:
        return False


def word_count(dstream):
    # counts = dstream.map(lambda v: json.loads(v)) \
    #     .map(lambda attr: attr['reviewText']) \
    #     .map(lambda line: line.split(' '))
    # counts.pprint()
    # result = counts.flatMap(lambda word: word)
    # result.pprint()
    #     # .filter(lambda word: check(word)) \
    #     # .map(lambda word: (word, 1)) \
    #     # .reduceByKey(lambda a, b: a + b)
    # return result

    parsed = dstream.map(lambda v: json.loads(v))
    # # parsed.pprint()
    new_result = parsed.map(lambda words: words['reviewText']).map(lambda line: line.split(" "))
    new_result.pprint()
    elem = new_result.flatMap(lambda word: word)
    elem.pprint()
    # # ["a","b"]
    ## counts = test_word.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
    # # counts.pprint()
    word = elem.filter(check)
    word.pprint()
    counts = elem.filter(check).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
    return counts


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

        results = word_count(dstream)
    else:
        print("Need input_stream and input_stream_port")
        sys.exit(1)

    # results.saveAsTextFiles(args.output)
    results.pprint()

    ssc.start()
    ssc.awaitTermination()
