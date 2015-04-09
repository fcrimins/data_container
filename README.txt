
data_container

Handles indexation of dates and entities.

Handles caching, given cache file names from DataStream.

Holds 2 (or 4 matrices).  (1) data, (2) date knowns, which act as masks on numpy
'data' arrays.  This means it must know about the current clock time.

The date_known 'matrix' actually doesn't really need to be a matrix, but rather
an interface to a matrix mask object so that date knowns can be stored as rules/logic
rather than data, if desired.

Interface:

def insert_item

def get_item i.e. []

Many types of underlying data:

(1) Entities of the universe entity type.  These have data stored in csr numpy arrays.

(2) Entities of other types.  These have data stored in sparse/mapped matrices.

(3) Matrices should be grow'able.  I.e. the out-of-bounds insert_items should expand
the underlying matrics and out-of-bounds get_items should return nan, rather than
raising exceptions.
