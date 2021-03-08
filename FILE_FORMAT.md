Baq File Format
===============

WORK IN PROGRESS :)

---

Backup consists of a metadata file and one or more data files (unless you are backing up an empty directory).

- baq.20210304T050607Z.metadata
- baq.20210304T050607Z.data-00000
- baq.20210304T050607Z.data-00001

The metadata file is a gzip-compressed [JSON-lines](https://jsonlines.org/) file.
Each line is JSON document, let's call it a _record_.

Header metadata record
----------------------

The first metadata file record acts as a header. It looks like this:

TODO

Subsequent metadata file records describe the backup contents.

Directory metadata record
-------------------------


Filey metadata record
---------------------

The `df_offset` and `df_size` describe location and size of this chunk data in a data file.
The name of the data file is stored in `df_name`.
