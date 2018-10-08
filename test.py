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