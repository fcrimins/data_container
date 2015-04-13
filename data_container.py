import numpy as np



class DateTimeIndexer(object):
    pass

class EntityIndexer(object):

    def __init__(self):
        self._type2indexer = {}

    def get_index(self, entity):
        try:
            idxr = self._type2indexer[type(entity)]
        except KeyError as _e:
            # @todo: have a unique mapping for insts in universe while other entity types can have their
            # indexers constructed on-the-fly
            self._type2indexer[type(entity)] = None
        return idxr[entity]

class Clock(object):
    pass

class DataContainer(object):

    def __init__(self, cache_file_root):
        self._dt_known_impl = None  # composition, e.g. data_dt + 1
        self._dt_indexer = None
        self._entity_indexer = None
        self._dt_idx_offset = 0

        # should we always use a dense numpy array and only convert to sparse before caching?
        def new_matrix(shape):
            m = np.empty(shape)
            m.fill(np.nan)
            return m
        self._matrix_type = new_matrix
        self._vstack = np.vstack  # for adding entities
        self._hstack = np.hstack  # for adding dates (or times)
        self._matrix = None

        self._cache_file_root = cache_file_root

    def __setitem__(self, key, value):
        entity, dt = key
        i = self._entity_indexer.get_index(entity)
        j = self._dt_indexer.get_index(dt) + self._dt_idx_offset

        try:
            self._matrix[i, j] = value
        except:
            nentities = self._entity_indexer.num_entities(type(entity))
            if self._matrix is None:
                self._matrix = self._matrix_type((nentities, 1))
                # @todo: set missing type to nan
                self._dt_idx_offset = -j
                self[key] = value  # recursive

            # if the entity indexer expanded to accommodate the new entity, then we should also
            elif i >= self._matrix.shape[0]:
                if i < nentities:
                    expand_by = nentities - self._matrix.shape[0]
                    ndates = self._matrix.shape[1]
                    self._matrix = self._vstack(self._matrix, self._matrix_type((expand_by, ndates)))
                    self[key] = value  # recursive
                else:
                    raise IndexError('Entity index {} out of bounds ({}) for entity {}'.format(i, self._matrix.shape[0], entity))

            # new dates
            # @todo: if this date fills out the remainder of a block, then perhaps we can attach ourselves
            # as an observer to the current iterator that's inserting these elements to be notified
            # when StopIteration occurs, at that point we can dump out to a cache file, likewise if this
            # date is the first element of a new block then we can check to see if a cache file exists
            else:
                ndates = self._matix.shape[1]
                # assert(j < 0 or j >= ndates)  # not necessarily, i could've been < 0
                if j < 0:
                    self._matrix = self._hstack(self._matrix_type((nentities, -j)), self._matrix)
                    self._dt_idx_offset -= j  # subtract here b/c j is negative
                    self[key] = value  # recursive
                elif j >= ndates:
                    self._matrix = self._hstack(self._matrix, self._matrix_type((nentities, j - ndates + 1)))
                    self[key] = value  # recursive
                else:
                    raise

    def __getitem__(self, item):
        """Doesn't cause any loading to occur if something isn't yet loaded.  Client DataStream
        must handle that.
        """
        entity, dt = item
        i = 0 if entity is None else self._entity_indexer.get_index(entity)
        j = self._dt_indexer.get_index(dt) + self._dt_idx_offset
        return self._matrix[i, j]

    def is_loaded(self, dt):
        try:
            self.__getitem__((None, dt))
        except:
            return False
        else:
            return True

    def load_cache_file(self, dt):
        """Load a cache file.  Returns False if the file doesn't exist or can't be loaded.
        Returns True if the load is successful.
        """
        block_id = self._get_block_id(dt)
        filename = '{}.{}.npy'.format(self._cache_file_root, block_id)
        try:
            newm = np.load(filename)
        except IOError:
            return False
        new_ncols = newm.shape[1]

        # figure out where to put newm
        j = self._dt_indexer.get_index(dt)
        if j < 0:
            # the block that was just loaded must extend far enough back to include j and everything in between
            if -j > new_ncols:
                raise RuntimeError('Attempting to load from a cache file ({}, {} columns) that is not immediately prior to the data that is currently loaded for datetime {} (index {})'.format(filename, new_ncols, dt, j))
            self._matrix = self._hstack(newm, self._matrix)
            self._dt_idx_offset += new_ncols

        else:
            assert(j >= self._matrix.shape[1])
            if j - self._matrix.shape[1] > new_ncols:
                raise RuntimeError('Attempting to load from a cache file ({}, {} columns) that is not immediately after the data that is currently loaded for datetime {} (index {})'.format(filename, new_ncols, dt, j))
            self._matrix = self._hstack(self._matrix, newm)

        return True

    def _get_block_id(self, dt):
        """Allows users to store blocks using different date formats."""
        return dt.year




