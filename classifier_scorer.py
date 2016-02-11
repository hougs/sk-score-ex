from argparse import ArgumentParser
from pyspark import SparkContext
from sklearn.externals import joblib
import sys
import os

# Parse commandline arguments
arg_parser = ArgumentParser()
arg_parser.add_argument("--model_path",
              action="store",
              help="Path to pickled model.")
arg_parser.add_argument("--data_path",
              action="store",
              help="HDFS path to data to apply model to.")
arg_parser.add_argument("--output_path",
              action="store",
              help="HDFS path to write model score file to.")
args = arg_parser.parse_args()


# Get a spark context
sc = SparkContext()

# Only break out of loop if we can load the model.
while True:
    # Check if path to pickled model exists
    if not os.path.exists(args.model_path):
        print "ERROR: Model path does not exist."
    try:
        # Try to load the saved model
        best_classifier = joblib.load(args.model_path)
    except IOError as e:
        print "I/O error({0}): {1}".format(e.errno, e.strerror)
    except:
        e = sys.exc_info()[0]
        print "Unexpected error. Could not load saved model. Error: %s" % e
    else:
        break

# Applies classifier to data
def apply_classifier(input_string, clf):
    id, input_string = input_string.split(",")
    score = clf.decision_function(input_row.features())
    label = clf.predict(input_row.features())
    # If the decision function is positive, the example is labeled 1.
    # label = 1 if score > 0 else 0
    return ",".join([id, score, label])


# If best_classifier is defined, try applying it to all of the data.
if best_classifier:
    # Load in input data as an rdd
    # assume the input is in the form id,input_string
    input_data = sc.textFile(args.data_path) 
    id_prediction = input_data.rdd.map(lambda row: apply_classifier(row, best_classifier))
    id_prediction.saveAsTextFile(args.output_path)

sc.stop()


