#Used ChatGPT to help with some parts of this HW

"""
Part 1: MapReduce

In our first part, we will practice using MapReduce
to create several pipelines.
This part has 20 questions.

As you complete your code, you can run the code with

    python3 part1.py
    pytest part1.py

and you can view the output so far in:

    output/part1-answers.txt

In general, follow the same guidelines as in HW1!
Make sure that the output in part1-answers.txt looks correct.
See "Grading notes" here:
https://github.com/DavisPL-Teaching/119-hw1/blob/main/part1.py

For Q5-Q7, make sure your answer uses general_map and general_reduce as much as possible.
You will still need a single .map call at the beginning (to convert the RDD into key, value pairs), but after that point, you should only use general_map and general_reduce.

If you aren't sure of the type of the output, please post a question on Piazza.
"""

# Spark boilerplate (remember to always add this at the top of any Spark file)
import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("DataflowGraphExample").getOrCreate()
sc = spark.sparkContext

# Additional imports
import pytest


"""
===== Questions 1-3: Generalized Map and Reduce =====

We will first implement the generalized version of MapReduce.
It works on (key, value) pairs:

- During the map stage, for each (key1, value1) pairs we
  create a list of (key2, value2) pairs.
  All of the values are output as the result of the map stage.

- During the reduce stage, we will apply a reduce_by_key
  function (value2, value2) -> value2
  that describes how to combine two values.
  The values (key2, value2) will be grouped
  by key, then values of each key key2
  will be combined (in some order) until there
  are no values of that key left. It should end up with a single
  (key2, value2) pair for each key.

1. Fill in the general_map function
using operations on RDDs.

If you have done it correctly, the following test should pass.
(pytest part1.py)

Don't change the q1() answer. It should fill out automatically.
"""

def general_map(rdd, f):
    """
    rdd: an RDD with values of type (k1, v1)
    f: a function (k1, v1) -> List[(k2, v2)]
    output: an RDD with values of type (k2, v2)
    """
    # TODO
    return rdd.flatMap(lambda kv: f(kv[0], kv[1]))

# Remove skip when implemented!
@pytest.mark.skip
def test_general_map():
    rdd = sc.parallelize(["cat", "dog", "cow", "zebra"])

    # Use first character as key
    rdd1 = rdd.map(lambda x: (x[0], x))

    # Map returning no values
    rdd2 = general_map(rdd1, lambda k, v: [])

    # Map returning length
    rdd3 = general_map(rdd1, lambda k, v: [(k, len(v))])
    rdd4 = rdd3.map(lambda pair: pair[1])

    # Map returnning odd or even length
    rdd5 = general_map(rdd1, lambda k, v: [(len(v) % 2, ())])

    assert rdd2.collect() == []
    assert sum(rdd4.collect()) == 14
    assert set(rdd5.collect()) == set([(1, ())])

def q1():
    # Answer to this part: don't change this
    rdd = sc.parallelize(["cat", "dog", "cow", "zebra"])
    rdd1 = rdd.map(lambda x: (x[0], x))
    rdd2 = general_map(rdd1, lambda k, v: [(1, v[-1])])
    return sorted(rdd2.collect())

"""
2. Fill in the reduce function using operations on RDDs.

If you have done it correctly, the following test should pass.
(pytest part1.py)

Don't change the q2() answer. It should fill out automatically.
"""

def general_reduce(rdd, f):
    """
    rdd: an RDD with values of type (k2, v2)
    f: a function (v2, v2) -> v2
    output: an RDD with values of type (k2, v2),
        and just one single value per key
    """
    # TODO
    return rdd.reduceByKey(f)

# Remove skip when implemented!
@pytest.mark.skip
def test_general_reduce():
    rdd = sc.parallelize(["cat", "dog", "cow", "zebra"])

    # Use first character as key
    rdd1 = rdd.map(lambda x: (x[0], x))

    # Reduce, concatenating strings of the same key
    rdd2 = general_reduce(rdd1, lambda x, y: x + y)
    res2 = set(rdd2.collect())

    # Reduce, adding lengths
    rdd3 = general_map(rdd1, lambda k, v: [(k, len(v))])
    rdd4 = general_reduce(rdd3, lambda x, y: x + y)
    res4 = sorted(rdd4.collect())

    assert (
        res2 == set([('c', "catcow"), ('d', "dog"), ('z', "zebra")])
        or res2 == set([('c', "cowcat"), ('d', "dog"), ('z', "zebra")])
    )
    assert res4 == [('c', 6), ('d', 3), ('z', 5)]

def q2():
    # Answer to this part: don't change this
    rdd = sc.parallelize(["cat", "dog", "cow", "zebra"])
    rdd1 = rdd.map(lambda x: (x[0], x))
    rdd2 = general_reduce(rdd1, lambda x, y: "hello")
    return sorted(rdd2.collect())

"""
3. Name one scenario where having the keys for Map
and keys for Reduce be different might be useful.

=== ANSWER Q3 BELOW ===
It might be useful to have the keys for Map and keys for Reduce be different because what if you want to organize
the data. By having different keys where Reduce could group the keys by their first letter and have the Map keys use the whole word as keys which would
make aggregating more flexible.
=== END OF Q3 ANSWER ===
"""

"""
===== Questions 4-10: MapReduce Pipelines =====

Now that we have our generalized MapReduce function,
let's do a few exercises.
For the first set of exercises, we will use a simple dataset that is the
set of integers between 1 and 1 million (inclusive).

4. First, we need a function that loads the input.
"""

def load_input(N=None, P=None):
    # Return a parallelized RDD with the integers between 1 and 1,000,000
    # This will be referred to in the following questions.
    # TODO
    if N is None:
        N = 1000000
    if P is None:
        rdd = sc.parallelize(range(1,N + 1))
    else:
        rdd = sc.parallelize(range(1,N + 1), P)
    return rdd

def q4(rdd):
    # Input: the RDD from load_input
    # Output: the length of the dataset.
    # You may use general_map or general_reduce here if you like (but you don't have to) to get the total count.
    # TODO
    rdd = load_input(1000000)
    return rdd.count()

"""
Now use the general_map and general_reduce functions to answer the following questions.

For Q5-Q7, your answers should use general_map and general_reduce as much as possible (wherever possible): you will still need a single .map call at the beginning (to convert the RDD into key, value pairs), but after that point, you should only use general_map and general_reduce.

5. Among the numbers from 1 to 1 million, what is the average value?
"""

def q5(rdd):
    # Input: the RDD from Q4
    # Output: the average value
    # TODO

    # Maps the data to counts and values
    mapped_data  = general_map(rdd.map(lambda x: (None, x)), lambda k, v: [(None, (1, v))])

    #Reduces data by adding up the counts and values
    reduced_data = general_reduce(mapped_data, lambda s, z: (s[0] + z[0], s[1] + z[1]))

    #Collects the result from reduced_data
    total_count, total_sum = reduced_data.collect()[0][1]
    
    return total_sum/total_count

"""
6. Among the numbers from 1 to 1 million, when written out,
which digit is most common, with what frequency?
And which is the least common, with what frequency?

(If there are ties, you may answer any of the tied digits.)

The digit should be either an integer 0-9 or a character '0'-'9'.
Frequency is the number of occurences of each value.

Your answer should use the general_map and general_reduce functions as much as possible.
"""

def q6(rdd):
    # Input: the RDD from Q4
    # Output: a tuple (most common digit, most common frequency, least common digit, least common frequency)
    # TODO
    
    #Map data
    mapped_data = general_map(rdd.map(lambda x: (None, x)), lambda k, v: [(d, 1) for d in str(v)])

    #Reduces data to add them together
    reduce_data = general_reduce(mapped_data, lambda s, z: s + z)

    #Creates a tuple
    digits = dict(reduce_data.collect())

    most_common_digit = max(digits, key = digits.get)
    least_common_digit = min(digits, key = digits.get)

    return (most_common_digit, digits[most_common_digit], least_common_digit, digits[least_common_digit])

"""
7. Among the numbers from 1 to 1 million, written out in English, which letter is most common?
With what frequency?
The least common?
With what frequency?

(If there are ties, you may answer any of the tied characters.)

For this part, you will need a helper function that computes
the English name for a number.

Please implement this without using an external library!
You should write this from scratch in Python.

Examples:

    0 = zero
    71 = seventy one
    513 = five hundred and thirteen
    801 = eight hundred and one
    999 = nine hundred and ninety nine
    1001 = one thousand one
    500,501 = five hundred thousand five hundred and one
    555,555 = five hundred and fifty five thousand five hundred and fifty five
    1,000,000 = one million

Notes:
- For "least frequent", count only letters which occur,
  not letters which don't occur.
- Please ignore spaces and hyphens.
- Use all lowercase letters.
- The word "and" should only appear after the "hundred" part, and nowhere else.
  It should appear after the hundreds if there are tens or ones in the same block.
  (Note the 1001 case above which differs from some other implementations!)
"""

# *** Define helper function(s) here ***


def number_to_english(n):
    #If n = 0 then it just returns zero
    if n == 0:
        return "zero"
    
    #Since our data goes up to one million and not like ninety-nine thousand, if n = 1,000,000 the output is one million separately
    if n == 1_000_000:
        return "one million"

    # Creates preexisting words that will be combined together later to create the appropriate number to English work
    units = ["", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine"]
    teens = ["ten", "eleven", "twelve", "thirteen", "fourteen", "fifteen", "sixteen", "seventeen", "eighteen", "nineteen"]
    tens = ["", "", "twenty", "thirty", "forty", "fifty", "sixty", "seventy", "eighty", "ninety"]

    parts = []

    #Handles the millions which will be used for q8b
    if n >= 1_000_000:
        millions = n // 1_000_000
        parts.append(number_to_english(millions) + " million")
        n %= 1_000_000
        if n > 0 and n < 100:
            parts.append("and")

    #This if line calculates for numbers that is 1,000 and above because it adds thousand at the end
    if n >= 1000:
        thousands = n // 1000
        parts.append(number_to_english(thousands) + " thousand")
        n %= 1000
        if n > 0 and n < 100:
            parts.append("and")

    #This if line calculates for numbers that is 100 and above because it adds hundred at the end
    if n >= 100:
        hundreds = n // 100
        parts.append(units[hundreds] + " hundred")
        n %= 100
        if n > 0:
            parts.append("and")

    #This if line calculates for numbers that is 20 and above because it adds like the twenty, thirty, etc. in the beginning
    if n >= 20:
        parts.append(tens[n // 10])
        n %= 10
        if n > 0:
            parts[-1] += "-" + units[n]

    #For when n is equal or more than 10 as they end with -teens
    elif n >= 10:
        parts.append(teens[n - 10])

    #For single digits
    elif n > 0:
        parts.append(units[n])

    return " ".join(parts).lower()


def q7(rdd):
    # Input: the RDD from Q4
    # Output: a tulpe (most common char, most common frequency, least common char, least common frequency)
    # TODO
   english_data = rdd.map(lambda x: number_to_english(x))

   letters_rdd = general_map(english_data.map(lambda word: (None, word)), lambda k, v: [(ch, 1) for ch in v if ch.isalpha()])

   counted_letters = general_reduce(letters_rdd, lambda a, b: a + b)

   dic_letters = dict(counted_letters.collect())
   most_common_char = max(dic_letters, key = dic_letters.get)
   least_common_char = min(dic_letters, key = dic_letters.get)

   return (most_common_char, dic_letters[most_common_char], least_common_char, dic_letters[least_common_char])

"""
8. Does the answer change if we have the numbers from 1 to 100,000,000?

Make a version of both pipelines from Q6 and Q7 for this case.
You will need a new load_input function.

Notes:
- The functions q8_a and q8_b don't have input parameters; they should call
  load_input_bigger directly.
- Please ensure that each of q8a and q8b runs in at most 3 minutes.
- If you are unable to run up to 100 million on your machine within the time
  limit, please change the input to 10 million instead of 100 million.
  If it is still taking too long even for that,
  you may need to change the number of partitions.
  For example, one student found that setting number of partitions to 100
  helped speed it up.
"""

def load_input_bigger(N=None, P=None):
    # TODO
    # Parallelizes data
    if N is None:
        N = 10000000
    if P is None:
        rdd = sc.parallelize(range(1,N + 1))
    else:
        rdd = sc.parallelize(range(1,N + 1), P)
    return rdd

def q8_a():
    # version of Q6
    # It should call into q6() with the new RDD!
    # Don't re-implemented the q6 logic.
    # Output: a tuple (most common digit, most common frequency, least common digit, least common frequency)
    # TODO
    rdd = load_input_bigger(10000000, 8)
    return q6(rdd)

def q8_b():
    # version of Q7
    # It should call into q7() with the new RDD!
    # Don't re-implemented the q6 logic.
    # Output: a tulpe (most common char, most common frequency, least common char, least common frequency)
    # TODO
    rdd = load_input_bigger(10000000, 8)
    return q7(rdd)

"""
Discussion questions

9. State what types you used for k1, v1, k2, and v2 for your Q6 and Q7 pipelines.

=== ANSWER Q9 BELOW ===
For Q6 I used: k1 = None, v1 = int (the original number from the rdd), k2 = strings of the digits, v2 = integer which adds up all the digits
that are the same
For Q7 I used k1 = None, v1 = string which is the english word of the numbers in rdd, k2 = string which makes each letter of the english word a key,
and v2 = integer that then adds up all of the other same letters
=== END OF Q9 ANSWER ===

10. Do you think it would be possible to compute the above using only the
"simplified" MapReduce we saw in class? Why or why not?

=== ANSWER Q10 BELOW ===
Yes, you can compute the above using only the "simplified" MapReduce we saw in class. The only problem with this is that (L1: map stage) 
it will produce exactly one output row which would then make it difficult to filter and makes it inefficient. In L2: reduce stage, we would also end
up with a single answer which would make it difficult if we wanted a more than one value as output.
=== END OF Q10 ANSWER ===
"""

"""
===== Questions 11-18: MapReduce Edge Cases =====

For the remaining questions, we will explore two interesting edge cases in MapReduce.

11. One edge case occurs when there is no output for the reduce stage.
This can happen if the map stage returns an empty list (for all keys).

Demonstrate this edge case by creating a specific pipeline which uses
our data set from Q4. It should use the general_map and general_reduce functions.

For Q11, Q14, and Q16:
your answer should return a Python set of (key, value) pairs after the reduce stage.
"""

def q11(rdd):
    # Input: the RDD from Q4
    # Output: the result of the pipeline, a set of (key, value) pairs
    # TODO

    #Maps the data
    mapped_data = general_map(rdd.map(lambda x: (None, x)), lambda k, v: [])

    #Reduces the data to give the value
    reduced_data = general_reduce(mapped_data, lambda k, val: val)

    return set(reduced_data.collect())

"""
12. What happened? Explain below.
Does this depend on anything specific about how
we chose to define general_reduce?

=== ANSWER Q12 BELOW ===
So, I got an output that says set(). It does not really depend on anything specific about how we chose to define general_reduce, but on
how we mapped the data out like where our mapper returned an empty list for each input. This produces the edge case where there is no output for the
reduce stage.
=== END OF Q12 ANSWER ===

13. Lastly, we will explore a second edge case, where the reduce stage can
output different values depending on the order of the input.
This leads to something called "nondeterminism", where the output of the
pipeline can even change between runs!

First, take a look at the definition of your general_reduce function.
Why do you imagine it could be the case that the output of the reduce stage
is different depending on the order of the input?

=== ANSWER Q13 BELOW ===
I think it is because it groups all values by key and then reduces by the function f. It is like first come first serve type of deal since spark splits
the data into partitions which will then be combined in different orders each time.
=== END OF Q13 ANSWER ===

14.
Now demonstrate this edge case concretely by writing a specific example below.
As before, you should use the same dataset from Q4.

Important: Please create an example where the output of the reduce stage is a set of (integer, integer) pairs.
(So k2 and v2 are both integers.)
"""

def q14(rdd):
    # Input: the RDD from Q4
    # Output: the result of the pipeline, a set of (key, value) pairs
    # TODO

    #Map the data to key-value pairing
    mapped_data = general_map(rdd.map(lambda x: (1, x)), lambda k, v: [(k, v)])

    #Reduce the data by summing for the keys 0, 1, and 2
    reduced_data = general_reduce(mapped_data, lambda a, b: a - b)

    #Returns a set of (integer, integer)
    return set(reduced_data.collect())

"""
15.
Run your pipeline. What happens?
Does it exhibit nondeterministic behavior on different runs?
(It may or may not! This depends on the Spark scheduler and implementation,
including partitioning.

=== ANSWER Q15 BELOW ===
I ran my pipeline multiple times and got the same values each time thus not showcasing a nondeterministic behavior. The set I got multiple times
is (1, 496079187472).
=== END OF Q15 ANSWER ===

16.
Lastly, try the same pipeline as in Q14
with at least 3 different levels of parallelism.

Write three functions, a, b, and c that use different levels of parallelism.
"""

def q16_a():
    # For this one, create the RDD yourself. Choose the number of partitions.
    # TODO
    rdd_eight = sc.parallelize(range(1, 500001), 8)
    return q14(rdd_eight)

def q16_b():
    # For this one, create the RDD yourself. Choose the number of partitions.
    # TODO
    rdd_twelve = sc.parallelize(range(1, 500001), 12)
    return q14(rdd_twelve)

def q16_c():
    # For this one, create the RDD yourself. Choose the number of partitions.
    # TODO
    rdd_sixteen = sc.parallelize(range(1, 500001), 16)
    return q14(rdd_sixteen)

"""
Discussion questions

17. Was the answer different for the different levels of parallelism?

=== ANSWER Q17 BELOW ===
Using the same rdd with all of my q16 with different levels of parallelism I do see different answers on each one. This is probably because
I use different number of partitions which changes the order in which the values enter the reducer, thus affecting the output, showing a
nondeterministic behavior.
=== END OF Q17 ANSWER ===

18. Do you think this would be a serious problem if this occured on a real-world pipeline?
Explain why or why not.

=== ANSWER Q18 BELOW ===
This would cause a serious problem if this occured on a real-world pipeline because the user might've wanted the opposite of a nondeterministic
behavior. With inconsistent output it may result in different events where conclusion on a data might not be accurate.
=== END OF Q18 ANSWER ===

===== Q19-20: Further reading =====

19.
The following is a very nice paper
which explores this in more detail in the context of real-world MapReduce jobs.
https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/icsecomp14seip-seipid15-p.pdf

Take a look at the paper. What is one sentence you found interesting?

=== ANSWER Q19 BELOW ===
I found this sentence interesting, "Between the mappers and reducers is a data shuffling stage where rows with the same reduce key are 
grouped together in to a sequence by a merge-sort." The reason I found this interesting is because I associated this with our previous question of
nondeterministic behavior. If the shuffling occurs each time, then the order in which the data comes in will change the output during the reduce
stage unless the output is the same each time.
=== END OF Q19 ANSWER ===

20.
Take one example from the paper, and try to implement it using our
general_map and general_reduce functions.
For this part, just return the answer "True" at the end if you found
it possible to implement the example, and "False" if it was not.
"""

def q20():
    # TODO
    #I did the example from figure 2 where the reduce function sums up the second column in all rows in a group

    #Maping each input x to make a row with group key and use general_map to emit each row as (group_key, row)
    
    #Creating the key and row
    rdd = sc.parallelize(range(1, 100001)) #This is just an example data
    keyed_data = rdd.map(lambda x: (x % 5, (x % 5, x)))

    general_mapped = general_map(keyed_data, lambda key, row: [(key, row)])

    #Reducer to sum up the second column
    def reducer_helper_add(key, rows):
        total = 0
        #Goes through each row that the key is associated with as row is a tuple
        for row in rows:
            #This adds the second column values together, not the first which would be row[0]
            total += row[1]
        return total
    
    reduced_data = general_reduce(general_mapped, reducer_helper_add)
    return True


"""
That's it for Part 1!

===== Wrapping things up =====

**Don't modify this part.**

To wrap things up, we have collected
everything together in a pipeline for you below.

Check out the output in output/part1-answers.txt.
"""

ANSWER_FILE = "output/part1-answers.txt"
UNFINISHED = 0

def log_answer(name, func, *args):
    try:
        answer = func(*args)
        print(f"{name} answer: {answer}")
        with open(ANSWER_FILE, 'a') as f:
            f.write(f'{name},{answer}\n')
            print(f"Answer saved to {ANSWER_FILE}")
    except NotImplementedError:
        print(f"Warning: {name} not implemented.")
        with open(ANSWER_FILE, 'a') as f:
            f.write(f'{name},Not Implemented\n')
        global UNFINISHED
        UNFINISHED += 1

def PART_1_PIPELINE():
    open(ANSWER_FILE, 'w').close()

    try:
        dfs = load_input()
    except NotImplementedError:
        print("Welcome to Part 1! Implement load_input() to get started.")
        dfs = sc.parallelize([])

    # Questions 1-3
    log_answer("q1", q1)
    log_answer("q2", q2)
    # 3: commentary

    # Questions 4-10
    log_answer("q4", q4, dfs)
    log_answer("q5", q5, dfs)
    log_answer("q6", q6, dfs)
    log_answer("q7", q7, dfs)
    log_answer("q8a", q8_a)
    log_answer("q8b", q8_b)
    # 9: commentary
    # 10: commentary

    # Questions 11-18
    log_answer("q11", q11, dfs)
    # 12: commentary
    # 13: commentary
    log_answer("q14", q14, dfs)
    # 15: commentary
    log_answer("q16a", q16_a)
    log_answer("q16b", q16_b)
    log_answer("q16c", q16_c)
    # 17: commentary
    # 18: commentary

    # Questions 19-20
    # 19: commentary
    log_answer("q20", q20)

    # Answer: return the number of questions that are not implemented
    if UNFINISHED > 0:
        print("Warning: there are unfinished questions.")

    return f"{UNFINISHED} unfinished questions"

if __name__ == '__main__':
    log_answer("PART 1", PART_1_PIPELINE)

"""
=== END OF PART 1 ===
"""
