import scipy



class DateIndexer(object):
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

        # @todo: or always use a dense numpy ndarray and only convert to sparse before caching?
        self._matrix_type = scipy.sparse.csr_matrix
        self._vstack = scipy.sparse.vstack
        self._hstack = scipy.sparse.hstack
        self._matrix = None

        self._cache_file_root = cache_file_root

    def __setitem__(self, key, value):
        entity, dt = key
        i = self._entity_indexer.get_index(entity)
        j = self._entity_indexer.get_index(dt) + self._dt_idx_offset

        try:
            self._matrix[i, j] = value
        except:
            nentities = self._entity_indexer.num_entities(type(entity))
            if self._matrix is None:
                self._matrix = self._matrix_type((nentities, 1))
                # @todo: set missing type to nan
                self._dt_idx_offset = -j
                self[key] = value  # recursive

            elif i >= self._matrix.shape[0]:
                # if the entity indexer expanded to accommodate the new entity, then we should also
                if i < nentities:
                    expand_by = nentities - self._matrix.shape[0]
                    ndates = self._matrix.shape[1]
                    self._matrix = self._vstack(self._matrix, self._matrix_type((expand_by, ndates)))
                    self[key] = value  # recursive
                else:
                    raise IndexError('Entity index {} out of bounds ({}) for entity {}'.format(i, self._matrix.shape[0], entity))

            else:
                ndates = self._matix.shape[1]
                assert(j < 0 or j >= ndates)
                if j < 0:
                    self._matrix = self._hstack(self._matrix_type((nentities, -j)), self._matrix)
                    self._dt_idx_offset -= j
                    self[key] = value  # recursive
                else:
                    self._matrix = self._hstack(self._matrix, self._matrix_type((nentities, j - ndates + 1)))
                    self[key] = value  # recursive







