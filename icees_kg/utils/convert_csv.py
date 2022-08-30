"""This is a throwaway script that makes value changes to existing csv files."""
import csv
import json


edge_file = './build/p_val_edges.csv'
newcsv = []
with open(edge_file) as csvfile:
    reader = csv.reader(csvfile)
    for i, row in enumerate(reader):
        newrow = []
        for column in row:
            if column == 'biolink:related_to':
                newrow.append('biolink:has_real_world_evidence_of_association_with')
            else:
                newrow.append(column)
        newcsv.append(newrow)
        # if i > 20:
        #     break

with open('./build/new_edges.csv', 'w') as newfile:
    writer = csv.writer(newfile)
    for row in newcsv:
        writer.writerow(row)
