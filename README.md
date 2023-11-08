# Max Word Count in a Book Using Apache Spark

This project demonstrates how to use Apache Spark to find the most frequently used word in a book. It is written in Python and uses PySpark.

## Overview

The script `max_word_count.py` performs the following operations:

1. Initialize a SparkContext.
2. Read the book text file into an RDD (Resilient Distributed Dataset).
3. Split the text into words and count the occurrences of each word.
4. Identifies the word with the maximum count in the dataset.
5. Prints the most frequently used word and the count.

