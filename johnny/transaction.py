import django
from django.db import transaction as django_transaction
from django.db import connection
try:
    from django.db import DEFAULT_DB_ALIAS
except:
    DEFUALT_DB_ALIAS = None

from johnny.decorators import wraps, available_attrs


class TransactionCache(object):
    '''
    TransactionCache is a wrapper around a cache backend that
    is transaction aware.

    It caches all values locally, and should therefore be flushed
    after each request.

    All changes are stored locally. On commit they are sent to
    the cache backend (e.g. memcached). On rollback the changes
    are discarded.

    Save points are also handled.
    '''
    NOT_THERE = object()

    def __init__(self, cache_backend):
        from johnny import cache, settings

        self.timeout = settings.MIDDLEWARE_SECONDS
        self.cache_backend = cache_backend

        # The cache is made up of many layers of dictionaries.
        # Each layer's values supersede those of the layers below.
        #
        # The first layer is the local cache which stores any
        # results retreived from the cache_backend.
        #
        # The second layer is keeps any changes made during a transaction.
        #
        # Subsequent layers are used by savepoints, so the changes can be
        # dropped on savepoint rollback.
        self.local_cache = {}
        self.stack = [{}, self.local_cache]
        self.savepoints = []

    def get(self, key, default=None):
        for layer in self.stack:
            if key in layer:
                value = layer[key]
                if value is self.NOT_THERE:
                    return default
                return value
        value = self.cache_backend.get(key, self.NOT_THERE)
        self.local_cache[key] = value
        if value is self.NOT_THERE:
            return default
        return value

    def get_many(self, keys):
        results = {}
        lookup = []
        for key in keys:
            for layer in self.stack:
                if key in layer:
                    value = layer[key]
                    if value is self.NOT_THERE:
                        break
                    results[key] = value
                    break
            else:
                lookup.append(key)
        if lookup:
            vars = self.cache_backend.get_many(lookup)
            for key in lookup:
                if key in vars:
                    self.local_cache[key] = vars[key]
                    results[key] = vars[key]
                else:
                    self.local_cache[key] = self.NOT_THERE
        return results

    def set(self, key, value, timeout=None):
        self.stack[0][key] = value

    def set_many(self, vars, timeout=None):
        self.stack[0].update(vars)

    def delete(self, key):
        self.stack[0][key] = self.NOT_THERE

    def delete_many(self, keys):
        for key in keys:
            self.stack[0][key] = self.NOT_THERE

    def rollback(self):
        self.local_cache.clear()
        self.stack[:-1] = [{}]

    def commit(self):
        # Send the changes on the stack, not including the first layer
        # as that is the local read cache.
        stack = self.stack[:-1]
        stack.reverse()

        vars = {}
        for layer in stack:
            vars.update(layer)

        deleted = []
        for key, value in vars.iteritems():
            if value is self.NOT_THERE:
                deleted.append(key)
        for key in deleted:
            del vars[key]

        if vars:
            self.cache_backend.set_many(vars, self.timeout)
        if deleted:
            self.cache_backend.delete_many(deleted)

        self.rollback()

    def savepoint(self, name):
        self.savepoints.insert(0, (name, len(self.stack)))
        self.stack.insert(0, {})

    def rollback_savepoint(self, name):
        sp_idx, stack_idx = self._find_savepoint(name)
        del self.savepoints[:sp_idx+1]
        del self.stack[:-stack_idx]

    def commit_savepoint(self, name):
        # Commiting a savepoint doesn't do anything to the data,
        # it just removes the savepoint information.
        # It is reasonable to combine all the layers up to the
        # savepoint into a single dictionary, but this isn't
        # neccessary and may in fact be slower than leaving it
        # alone.
        sp_idx, stack_idx = self._find_savepoint(name)
        del self.savepoints[:sp_idx+1]

    def _find_savepoint(self, name):
        for sp_idx, (sp, stack_idx) in enumerate(self.savepoints):
            if sp == name:
                return sp_idx, stack_idx
        raise IndexError()


class TransactionManager(object):
    """
    TransactionManager hooks a TransactionCache into Django's
    transaction system.
    """
    _patched_var = False

    def __init__(self, cache_backend, keygen):
        from johnny import cache, settings

        self.timeout = settings.MIDDLEWARE_SECONDS
        self.prefix = settings.MIDDLEWARE_KEY_PREFIX

        self.cache_backend = cache_backend
        self.tx_cache = TransactionCache(self.cache_backend)
        self.keygen = keygen(self.prefix)
        self._originals = {}

    def is_managed(self, using=None):
        if django.VERSION[1] < 2:
            return django_transaction.is_managed()
        return django_transaction.is_managed(using=using)

    def get(self, key, default=None, using=None):
        return self.tx_cache.get(key, default)

    def get_many(self, keys, using=None):
        return self.tx_cache.get_many(keys)

    def set(self, key, val, timeout=None, using=None):
        """
        Set will be using the generational key, so if another thread
        bumps this key, the localstore version will still be invalid.
        If the key is bumped during a transaction it will be new
        to the global cache on commit, so it will still be a bump.
        """
        if timeout is None:
            timeout = self.timeout
        if self.is_managed(using=using) and self._patched_var:
            self.tx_cache.set(key, val, timeout)
        else:
            self.cache_backend.set(key, val, timeout)

    def commit(self, using=None):
        self.tx_cache.commit()

    def rollback(self, using=None):
        self.tx_cache.rollback()

    def _patched(self, original, commit=True, unless_managed=False):
        @wraps(original, assigned=available_attrs(original))
        def newfun(using=None):
            #1.2 version
            original(using=using)
            # copying behavior of original func
            # if it is an 'unless_managed' version we should do nothing if transaction is managed
            if not unless_managed or not self.is_managed(using=using):
                if commit:
                    self.commit(using=using)
                else:
                    self.rollback(using=using)

        return newfun

    def _uses_savepoints(self):
        return connection.features.uses_savepoints

    def _create_savepoint(self, sid, using=None):
        self.tx_cache.savepoint(sid)

    def _rollback_savepoint(self, sid, using=None):
        self.tx_cache.rollback_savepoint(sid)

    def _commit_savepoint(self, sid, using=None):
        self.tx_cache.commit_savepoint(sid)

    def _savepoint(self, original):
        @wraps(original, assigned=available_attrs(original))
        def newfun(using=None):
            if using != None:
                sid = original(using=using)
            else:
                sid = original()
            if self._uses_savepoints():
                self._create_savepoint(sid, using)
            return sid
        return newfun

    def _savepoint_rollback(self, original):
        def newfun(sid, *args, **kwargs):
            original(sid, *args, **kwargs)
            if self._uses_savepoints():
                if len(args) == 2:
                    using = args[1]
                else:
                    using = kwargs.get('using', None)
                self._rollback_savepoint(sid, using)
        return newfun

    def _savepoint_commit(self, original):
        def newfun(sid, *args, **kwargs):
            original(sid, *args, **kwargs)
            if self._uses_savepoints():
                if len(args) == 1:
                    using = args[0]
                else:
                    using = kwargs.get('using', None)
                self._commit_savepoint(sid, using)
        return newfun

    def _getreal(self, name):
        return getattr(django_transaction, 'real_%s' % name,
                getattr(django_transaction, name))

    def patch(self):
        """
        This function monkey patches commit and rollback
        writes to the cache should not happen until commit (unless our state
        isn't managed). It does not yet support savepoints.
        """
        if not self._patched_var:
            self._originals['rollback'] = self._getreal('rollback')
            self._originals['rollback_unless_managed'] = self._getreal('rollback_unless_managed')
            self._originals['commit'] = self._getreal('commit')
            self._originals['commit_unless_managed'] = self._getreal('commit_unless_managed')
            self._originals['savepoint'] = self._getreal('savepoint')
            self._originals['savepoint_rollback'] = self._getreal('savepoint_rollback')
            self._originals['savepoint_commit'] = self._getreal('savepoint_commit')
            django_transaction.rollback = self._patched(django_transaction.rollback, False)
            django_transaction.rollback_unless_managed = self._patched(django_transaction.rollback_unless_managed,
                                                                       False, unless_managed=True)
            django_transaction.commit = self._patched(django_transaction.commit, True)
            django_transaction.commit_unless_managed = self._patched(django_transaction.commit_unless_managed,
                                                                     True, unless_managed=True)
            django_transaction.savepoint = self._savepoint(django_transaction.savepoint)
            django_transaction.savepoint_rollback = self._savepoint_rollback(django_transaction.savepoint_rollback)
            django_transaction.savepoint_commit = self._savepoint_commit(django_transaction.savepoint_commit)

            self._patched_var = True

    def unpatch(self):
        for fun in self._originals:
            setattr(django_transaction, fun, self._originals[fun])
        self._patched_var = False
