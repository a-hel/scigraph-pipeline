import csv

def article_uri_generator(lookup_filename):
    with open(lookup_filename, newline='') as csvfile:
        reader = csv.reader(csvfile, delimiter=',', quotechar='"')
        next(reader)
        for row in reader:
            if not row[7]:
                continue
            yield {"doi": row[7], "uri": f"assets/articles/{row[8]}.nxml"}