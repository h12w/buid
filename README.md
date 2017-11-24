buid: Bipartite Unique Identifier
=================================

A BUID is a 128-bit unique ID composed of two 64-bit parts: shard and key.

It is not only a unique ID, but also contains the sharding information, so that the messages with the same BUID could be stored together within the same DB shard.

Also, when a message is stored in a shard, the shard part of the BUID can be trimmed off to save the space, and only the key part needs to be stored as the primary key.

Bigendian is chosen to make each part byte-wise lexicographic sortable.

The string representation uses [basex](https://github.com/eknkc/basex) 62 encoding.

TODO:

* monotonic clock to pretect ID generation from clock going backward
