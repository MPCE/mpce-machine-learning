# Import necessary libraries
from future.builtins import next
import os
import csv
import re
import logging
import optparse
import dedupe
import pandas as pd

# Paths to key files
input_file = "data/reduced_super_book.csv"
output_file = "data/dedupe_matches.csv"
settings_file = "data/dedupe_learned_settings.csv"
training_file = "data/dedupe_training.json"

# Data was preprocessed in R.
# Read in the csv:
df = pd.read_csv(input_file)
# Replace 'Nan' with 'None' object (required by Dedupe)
df = df.where((pd.notnull(df)), None)
# Convert into dictionary of records
data = df.to_dict(orient="index")

# Create a variables list for Dedupe to work with
variables = [
    {'field':'primary_author_name','type':'String'},
    {'field':'full_book_title','type':'String'}
]

# Initialise deduplication object
deduper = dedupe.Dedupe(variables)

# Train the model
deduper.sample(data, 150000, .5) # This randomly generates 150000 pairs of records to compare
dedupe.consoleLabel(deduper) # Call up command line tool for manually labelling training pairs.
deduper.train() # Trains the model to work out which pairs are the same

# Write training to disk
with open(training_file, 'w') as tf:
	deduper.writeTraining(tf)

with open(settings_file, 'wb') as sf:
	deduper.writeSettings(sf)

# Now make our predictions
threshold = deduper.threshold(data, recall_weight = 1) # We care equally about precision and recall

print("clustering ...")
clustered_dupes = deduper.match(data, threshold)

print("# duplicate sets", len(clustered_dupes))

# Write the predictions to csv
cluster_membership = {}
cluster_id = 0
for (cluster_id, cluster) in enumerate(clustered_dupes):
    id_set, scores = cluster
    cluster_d = [data[c] for c in id_set]
    canonical_rep = dedupe.canonicalize(cluster_d)
    for record_id, score in zip(id_set, scores):
        cluster_membership[record_id] = {
            "cluster id" : cluster_id,
            "canonical representation" : canonical_rep,
            "confidence": score
        }

singleton_id = cluster_id + 1
row_id = -1

with open(output_file, 'w', newline='') as f_output, open(input_file) as f_input:
	writer = csv.writer(f_output)
	reader = csv.reader(f_input)
	heading_row = next(reader)
	heading_row.insert(0, 'confidence_score')
	heading_row.insert(0, 'Cluster ID')
	canonical_keys = canonical_rep.keys()
	for key in canonical_keys:
		heading_row.append('canonical_' + key)

	writer.writerow(heading_row)

	for row in reader:
		row_id += 1 # increment row id
		if row_id in cluster_membership:
			cluster_id = cluster_membership[row_id]["cluster id"]
			canonical_rep = cluster_membership[row_id]["canonical representation"]
			row.insert(0, cluster_membership[row_id]['confidence'])
			row.insert(0, cluster_id)
			for key in canonical_keys:
				row.append(canonical_rep[key].encode('utf8'))
		else:
			row.insert(0, None)
			row.insert(0, singleton_id)
			singleton_id += 1
			for key in canonical_keys:
				row.append(None)
		writer.writerow(row)