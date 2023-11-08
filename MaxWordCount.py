from pyspark import SparkContext, SparkConf

def main():
    # Create Spark context
    conf = SparkConf().setAppName('MaxWordCount')
    sc = SparkContext(conf=conf)

    # Read text file into RDD
    book_text = sc.textFile('pg6761.txt')

    # Split lines into words, transform into word pairs, and count the words
    words = book_text.flatMap(lambda line: line.split())
    word_pairs = words.map(lambda word: (word, 1))
    word_counts = word_pairs.reduceByKey(lambda a, b: a + b)

    # Word with highest count
    max_word = word_counts.max(lambda pair: pair[1])

    # Print the word with its count
    print(f"Word that occured maximum times : {max_word[0]} occured {max_word[1]} times.")

    # Stop Spark context
    sc.stop()

if __name__ == "__main__":
    main()
