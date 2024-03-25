# cz - Cloud Zip

list and get specific files from remote zip archives without downloading the whole thing 

> [!NOTE] 
> currently, S3 is the only supported backend

## Installation

Clone and build the project (no binaries available atm, sorry!)

```shell
git clone https://github.com/ozkatz/cloudzip.git
cd cloudzip
go build -o cz main.go
```

Then copy the `cz` binary into a location in your `$PATH`

```shell
cp cz /usr/local/bin/
```

## Usage

Listing the contents of a zip file without downloading it:

```shell
cz ls "s3://example-bucket/path/to/archive.zip"
```

Downloading and extracting a specific object from within a zip file:

```shell
cz cat "s3://example-bucket/path/to/archive.zip" "images/cat.png" > cat.png
```

## Why does this exist?

My use case was a pretty specific access pattern:

> Upload lots of small (~1-100Kb) files as quickly as possible, while still allowing random access to them

How does `cz` solve this? 

Well, uploading many small files to object stores is hard to do efficiently. 

Bundling them as a large object
and using [multipart uploads](https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html) to parallelize the upload while retaining bigger chunks is the most efficient way.

While this is commonly done with `tar`, the [tar format](https://www.loc.gov/preservation/digital/formats/fdd/fdd000531.shtml) doesn't keep an index of the files included in it. 
Scanning the archive until we find the file we're looking for means we might end up downloading the whole thing.

Zip, on the other hand, has a [central directory](https://en.wikipedia.org/wiki/ZIP_(file_format)), which is an index! It stores paths in the archive and their offset in the file. 

This index, together with [byte range requests](https://developer.mozilla.org/en-US/docs/Web/HTTP/Range_requests) (supported by [all](https://docs.aws.amazon.com/whitepapers/latest/s3-optimizing-performance-best-practices/use-byte-range-fetches.html) [major](https://learn.microsoft.com/en-us/rest/api/storageservices/specifying-the-range-header-for-blob-service-operations) [object stores](https://cloud.google.com/storage/docs/samples/storage-download-byte-range)), allow reading a small file(s) from large archives without having to fetch the entire thing!

We can even write a zip file directly to remote storage without saving it locally:

```shell
zip -r - -0 * | aws s3 cp - "s3://example-bucket/path/to/archive.zip"
```

#### but what about CPU usage? Won't compression slow down the upload?

Zip files don't have to be compressed! `zip -0` will result in an uncompressed archive, so there's no additional overhead.

## How Does it Work?

`cz ls` performs 2 [HTTP range requests](https://developer.mozilla.org/en-US/docs/Web/HTTP/Range_requests):

1. Fetch the last 64kB of the zip file, looking for the End Of Central Directory ([EOCD](https://en.wikipedia.org/wiki/ZIP_(file_format)#End_of_central_directory_record_(EOCD))) Marker  (and/or [EOCD64](https://en.wikipedia.org/wiki/ZIP_(file_format)#ZIP64)). 
1. The marker contains the exact start offset and size of the [Central Directory](https://en.wikipedia.org/wiki/ZIP_(file_format)#Central_directory_file_header), which is then read by issuing another HTTP range request

Once the central directory is read, it is parsed and written to `stdout`, similar to the output of `unzip -l`.

`cz cat` does the same thing - read the central directory, looking for the file we wish to download. Once found, we read the byte offset and size of the file, issuing a third HTTP range request, fetching the file itself.

Because zip files store each file (whether compressed or not) independently, this is enough to uncompress and write the file to `stdout`.
