from pyspark import SparkContext, SparkConf

def main():
    # Create Spark context with necessary configuration
    conf = SparkConf().setAppName('MaxWordCount')
    sc = SparkContext(conf=conf)

    # Read the text file into an RDD
    book_text = sc.textFile('path/to/book.txt')

    # Split the lines into words, transform into word pairs, and count the words
    words = book_text.flatMap(lambda line: line.split())
    word_pairs = words.map(lambda word: (word, 1))
    word_counts = word_pairs.reduceByKey(lambda a, b: a + b)

    # Find the word with the highest count
    max_word = word_counts.max(lambda pair: pair[1])

    # Print the word with the highest count
    print(f"The most frequently used word is: {max_word[0]} with {max_word[1]} occurrences.")

    # Stop the Spark context
    sc.stop()

if __name__ == "__main__":
    main()
