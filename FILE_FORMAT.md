Baq File Format
===============

WORK IN PROGRESS :)

---

Backup consists of a metadata file and one or more data files (unless you are backing up an empty directory).

- baq.20210304T050607Z.metadata
- baq.20210304T050607Z.data-00000
- baq.20210304T050607Z.data-00001


Encryption
----------

(Encryption is optional, the following text applies for backups when encryption is enabled.)

New AES key is generated for each individual backup.
All data chunks are encrypted unsing this AES key.

The AES key is encrypted using `age --armor` and the encrypted form is stored in metadata file header record.

The metadata file header record contains all AES keys that could be necessary for a complete backup restore,
including the AES keys from previous backups.
It is because the current backup can point to data chunks stored in data files of some previous backup,
which would be of course encrypted with the AES key generated for that previous backup.


Metadata file
-------------

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
