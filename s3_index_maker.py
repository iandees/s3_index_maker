import argparse
import boto3
import io
import pprint
import shutil
from urllib.parse import urlparse

def sizeof_fmt(num, suffix='B'):
    for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)

def format_index(entries):
    buf = io.StringIO()
    buf.write("<html><head></head><body>\n")
    buf.write("<table>\n")
    buf.write("<tr><th></th><th>Name</th><th>Last modified</th><th>Size</th></tr>\n")

    buf.write("<tr><th colspan='4'><hr/></th></tr>\n")
    buf.write("<tr><td>&lt;</td><td><a href='../index.html'>Parent Directory</a></td><td>&nbsp;</td><td>&nbsp;</td></tr>\n")

    for entry in entries:
        last_mod = entry.get('last_modified')
        if last_mod:
            last_mod = last_mod.isoformat()
        else:
            last_mod = "-"

        size = entry.get('size')
        if size:
            size = sizeof_fmt(size)
        else:
            size = "-"

        buf.write("<tr><td>{type}</td><td><a href='{href}'>{name}</a></td><td>{last_modified}</td><td>{size}</td></tr>\n".format(
            type="D" if entry['type'] == 'directory' else "F",
            href=entry['href'],
            name=entry['name'],
            last_modified=last_mod,
            size=size,
        ))

    buf.write("</table>\n")
    buf.write("</body></html>\n")
    buf.seek(0)

    return buf

def process_prefix(client, bucket, prefix):
    paginator = client.get_paginator('list_objects_v2')

    response_iter = paginator.paginate(
        Bucket=bucket,
        Prefix=prefix,
        Delimiter='/',
    )

    recurse_into = []
    entries = []
    for a in response_iter:
        for d in a.get('CommonPrefixes', []):
            entries.append({
                "type": "directory",
                "name": d['Prefix'].rsplit('/')[-2] + "/",
                "href": d['Prefix'][len(prefix):] + 'index.html',
            })

            recurse_into.append(d['Prefix'])

        for f in a.get('Contents', []):
            filename = f['Key'].rsplit('/')[-1]

            if filename == 'index.html':
                # Don't include the index.html file in the index
                continue

            entries.append({
                "type": "file",
                "name": filename,
                "href": f['Key'],
                "size": f['Size'],
                "last_modified": f['LastModified'],
            })

    output = format_index(entries)
    client.put_object(
        Bucket=bucket,
        Key=prefix + "index.html",
        ACL='public-read',
        ContentType="text/html",
        Body=output.getvalue().encode('utf8'),
    )
    print("Wrote index to https://s3.amazonaws.com/%s/%s" % (bucket, prefix + "index.html"))
    del output

    for prefix in recurse_into:
        process_prefix(client, bucket, prefix)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('prefix', help="S3 prefix (including s3://<bucketname>) to build an index from")
    args = parser.parse_args()

    pr = urlparse(args.prefix)
    assert pr.scheme == 's3', "The S3 prefix argument must start with s3://"
    assert pr.netloc, "You must specify a bucket name with s3://<bucketname>"

    bucket = pr.netloc
    prefix = pr.path[1:]

    print("Processing bucket '%s', prefix '%s'" % (bucket, prefix))

    client = boto3.client('s3')
    process_prefix(client, bucket, prefix)

if __name__ == '__main__':
    main()
