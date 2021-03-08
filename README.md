Baq – incremental backup tool with compression and encryption
=============================================================

Supports backing up to:

- local directory
- AWS S3 bucket

All backed-up files are split into blocks (1 MB size, but can be configured).
Hash checksum is computed for every block.
Only blocks with a checksum not present in previous backups are stored in the new backup.
Blocks are compressed and encrypted before being stored into a backup data file.
The encryption is asymetric – currently the [age](https://age-encryption.org/) program is used, so you can use even your SSH key.
Metadata (file name, size and hash) are not encrypted.

Primary goals of this project:

- AWS S3 (and equivalent alternative cloud storage) "friendlinnes" – data files on S3 can be stored in DEEP_ARCHIVE storage class
- asymmetric cryptography - the backed-up machine knows only the public key(s)
- scripting & automation friendly – no interactive prompts
- easy installation (not a single static binary unfortunately, but any distribution-default Python should suffice)
- suitable for backing up terabytes of database backups (with a lot of multi-gigabyte files)
- able to work with disk devices, LVM volumes and snapshots


What this project does NOT do (at least right now)
--------------------------------------------------

As of right now, `baq` is probably not the best tool to back up your homedir.
Various filesystems, file types and file/directory metadata combinations are not well tested yet.

Currently, the primary backup destination ("storage backend") is AWS S3.
Navigating the AWS web console is pretty unfriendly for uninitiated people.


Alternatives
------------

### Restic

[restic.net](https://restic.net/)
[restic.readthedocs.io](https://restic.readthedocs.io/)

Restic was a primary inspiration for this project.

The goal of `baq` is to provide similar functionality like Restic:

- incremental backup implemented through hash checksums of small data blocks
- encryption

But I wanted to do a few things differently:

- use assymetric encryption (public key for encryption/backup, private key for decrypt/restore)
- use compression
- store the data blocks in a big file so it is easier to manipulate (including less per-request cost on AWS S3)
- as simple as possible


### Duplicity

[duplicity.nongnu.org](http://duplicity.nongnu.org/)

I had used Duplicity previously.
I liked the incremental backups (duplicity uses librsync with rolling hash algorithm), encryption (based on GPG) and multiple backends (including AWS S3).
Unfortunately I have experienced performance issues when backing up multi-gigabyte files (database data) incrementally.
I've decided to use `age` instead of GPG as encryption tool for `baq`.


### rdiff-backup

Another backup tool using librsync.
Basically it copies all data to remote system as they are + keeps reverse diffs to be able to retrieve older versions of those files.
No encryption, no compression (unless you have encrypted and/or compressed filesystem on that remote system.)
Not friendly with cloud storage like AWS S3.


### Git

Git is a version control tool, not a backup tool – but can be used as one.
Git uses some kind of delta compression which is very space effective for git's main use case, but also very slow on large files.
Also it keeps the whole history on the "source" machine.
I personally use git sometimes to backup files and track changes in the /etc directory.


### Others

There is a lot of backup tools – see for example this overview: https://wiki.archlinux.org/index.php/Synchronization_and_backup_programs

It is pretty possible that some tool, or some newly emerged tool, does a similar thing as `baq`.
We can talk about it in [discussions](https://github.com/messa/baq/discussions).
