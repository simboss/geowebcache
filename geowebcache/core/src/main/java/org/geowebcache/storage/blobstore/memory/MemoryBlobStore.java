/**
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.geowebcache.storage.blobstore.memory;

import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.geowebcache.io.ByteArrayResource;
import org.geowebcache.io.Resource;
import org.geowebcache.storage.BlobStore;
import org.geowebcache.storage.BlobStoreListener;
import org.geowebcache.storage.StorageException;
import org.geowebcache.storage.TileObject;
import org.geowebcache.storage.TileRange;
import org.geowebcache.storage.blobstore.memory.guava.GuavaCacheProvider;

/**
 * This class is an implementation of the {@link BlobStore} interface wrapping another {@link BlobStore} implementation and supporting in memory
 * caching. Caching is provided by an input {@link CacheProvider} object.
 * 
 * @author Nicola Lagomarsini Geosolutions
 */
public class MemoryBlobStore implements BlobStore {

    /** {@link Log} object used for logging exceptions */
    private final static Log LOG = LogFactory.getLog(MemoryBlobStore.class);

    /** {@link BlobStore} to use when no element is found */
    private BlobStore store;

    /** {@link CacheProvider} object to use for caching */
    private CacheProvider cacheProvider;

    /** Executor service used for scheduling cacheProvider store operations like put,delete,... */
    private final ExecutorService executorService;

    /** {@link ReentrantReadWriteLock} used for handling concurrency when accessing the cacheProvider */
    private final ReentrantReadWriteLock lock;

    /** {@link WriteLock} used for scheduling the access to the {@link MemoryBlobStore} state */
    private final WriteLock writeLock;

    /** {@link ReadLock} used for granting access to operations which does not change the {@link MemoryBlobStore} state */
    private final ReadLock readLock;

    public MemoryBlobStore() {
        // Initialization of the various elements
        this.executorService = Executors.newFixedThreadPool(1);
        lock = new ReentrantReadWriteLock(true);
        writeLock = lock.writeLock();
        readLock = lock.readLock();
        // Initialization of the cacheProvider and store. Must be overridden, this uses default and caches in memory
        setStore(new NullBlobStore());
        GuavaCacheProvider startingCache = new GuavaCacheProvider();
        startingCache.setConfiguration(new CacheConfiguration());
        setCacheProvider(startingCache);
    }

    @Override
    public boolean delete(String layerName) throws StorageException {
        // ReadLock is used because we are changing the internal state of the cacheProvider, not the MemoryBlobStore state
        readLock.lock();
        try {
            // Remove from cacheProvider
            cacheProvider.removeLayer(layerName);
            // Remove the layer. Wait other scheduled tasks
            Future<Boolean> future = executorService.submit(new BlobStoreTask(store,
                    BlobStoreAction.DELETE_LAYER, layerName));
            // Variable containing the execution result
            boolean executed = false;
            try {
                // Waiting tasks
                executed = future.get();
            } catch (InterruptedException e) {
                if (LOG.isErrorEnabled()) {
                    LOG.error(e.getMessage(), e);
                }
            } catch (ExecutionException e) {
                if (LOG.isErrorEnabled()) {
                    LOG.error(e.getMessage(), e);
                }
            }
            // Returns the result
            return executed;
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public boolean deleteByGridsetId(String layerName, String gridSetId) throws StorageException {
        // ReadLock is used because we are changing the internal state of the cacheProvider, not the MemoryBlobStore state
        readLock.lock();
        try {
            // Remove the layer from the cacheProvider
            cacheProvider.removeLayer(layerName);
            // Remove selected gridsets
            executorService.submit(new BlobStoreTask(store, BlobStoreAction.DELETE_GRIDSET,
                    layerName, gridSetId));
            return true;
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public boolean delete(TileObject obj) throws StorageException {
        // ReadLock is used because we are changing the internal state of the cacheProvider, not the MemoryBlobStore state
        readLock.lock();
        try {
            // Remove from cacheProvider
            cacheProvider.removeTileObj(obj);
            // Remove selected TileObject
            executorService.submit(new BlobStoreTask(store, BlobStoreAction.DELETE_SINGLE, obj));
            return true;
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public boolean delete(TileRange obj) throws StorageException {
        // ReadLock is used because we are changing the internal state of the cacheProvider, not the MemoryBlobStore state
        readLock.lock();
        try {
            // flush the cacheProvider
            cacheProvider.clear();
            // Remove selected TileRange
            executorService.submit(new BlobStoreTask(store, BlobStoreAction.DELETE_RANGE, obj));
            return true;
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public boolean get(TileObject obj) throws StorageException {
        // ReadLock is used because we are changing the internal state of the cacheProvider, not the MemoryBlobStore state
        readLock.lock();
        try {
            TileObject cached = cacheProvider.getTileObj(obj);
            boolean found = false;
            if (cached == null) {
                // Try if it can be found in the system. Wait other scheduled tasks
                Future<Boolean> future = executorService.submit(new BlobStoreTask(store,
                        BlobStoreAction.GET, obj));
                try {
                    // Waiting threads
                    found = future.get();
                } catch (InterruptedException e) {
                    if (LOG.isErrorEnabled()) {
                        LOG.error(e.getMessage(), e);
                    }
                } catch (ExecutionException e) {
                    if (LOG.isErrorEnabled()) {
                        LOG.error(e.getMessage(), e);
                    }
                }
                // If the file has been found, it is inserted in cacheProvider
                if (found) {
                    // Get the Cached TileObject
                    cached = getByteResourceTile(obj);
                    // Put the file in Cache
                    cacheProvider.putTileObj(cached);
                }
            } else {
                // Found in cacheProvider
                found = true;
            }
            // If found add its resource to the input TileObject
            if (found) {
                Resource resource = cached.getBlob();
                obj.setBlob(resource);
                obj.setCreated(resource.getLastModified());
                obj.setBlobSize((int) resource.getSize());
            }

            return found;
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void put(TileObject obj) throws StorageException {
        // ReadLock is used because we are changing the internal state of the cacheProvider, not the MemoryBlobStore state
        readLock.lock();
        try {
            TileObject cached = getByteResourceTile(obj);
            cacheProvider.putTileObj(cached);
            // Add selected TileObject. Wait other scheduled tasks
            Future<Boolean> future = executorService.submit(new BlobStoreTask(store,
                    BlobStoreAction.PUT, obj));
            // Variable containing the execution result
            try {
                future.get();
            } catch (InterruptedException e) {
                if (LOG.isErrorEnabled()) {
                    LOG.error(e.getMessage(), e);
                }
            } catch (ExecutionException e) {
                if (LOG.isErrorEnabled()) {
                    LOG.error(e.getMessage(), e);
                }
            }
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void clear() throws StorageException {
        // ReadLock is used because we are changing the internal state of the cacheProvider, not the MemoryBlobStore state
        readLock.lock();
        try {
            // flush the cacheProvider
            cacheProvider.clear();
            // Remove all the files
            executorService.submit(new BlobStoreTask(store, BlobStoreAction.CLEAR, ""));
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void destroy() {
        // WriteLock is used because we are changing the MemoryBlobStore state
        writeLock.lock();
        try {
            // flush the cacheProvider
            cacheProvider.reset();
            // Remove all the files
            Future<Boolean> future = executorService.submit(new BlobStoreTask(store,
                    BlobStoreAction.DESTROY, ""));
            // Variable containing the execution result
            try {
                future.get();
            } catch (InterruptedException e) {
                if (LOG.isErrorEnabled()) {
                    LOG.error(e.getMessage(), e);
                }
            } catch (ExecutionException e) {
                if (LOG.isErrorEnabled()) {
                    LOG.error(e.getMessage(), e);
                }
            }
            // Stop the pending tasks
            executorService.shutdownNow();
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void addListener(BlobStoreListener listener) {
        // ReadLock is used because we are changing the internal state of the store, not the MemoryBlobStore state
        readLock.lock();
        try {
            // Add a new Listener
            store.addListener(listener);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public boolean removeListener(BlobStoreListener listener) {
        // ReadLock is used because we are changing the internal state of the store, not the MemoryBlobStore state
        readLock.lock();
        try {
            // Remove a listener
            return store.removeListener(listener);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public boolean rename(String oldLayerName, String newLayerName) throws StorageException {
        // ReadLock is used because we are changing the internal state of the cacheProvider, not the MemoryBlobStore state
        readLock.lock();
        try {
            // flush the cacheProvider
            cacheProvider.clear();
            // Rename the layer. Wait other scheduled tasks
            Future<Boolean> future = executorService.submit(new BlobStoreTask(store,
                    BlobStoreAction.RENAME, oldLayerName, newLayerName));
            // Variable containing the execution result
            boolean executed = false;
            try {
                executed = future.get();
            } catch (InterruptedException e) {
                if (LOG.isErrorEnabled()) {
                    LOG.error(e.getMessage(), e);
                }
                executed = false;
            } catch (ExecutionException e) {
                if (LOG.isErrorEnabled()) {
                    LOG.error(e.getMessage(), e);
                }
                executed = false;
            }
            return executed;
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public String getLayerMetadata(String layerName, String key) {
        // ReadLock is used because we are changing the internal state of the store, not the MemoryBlobStore state
        readLock.lock();
        try {
            // Get the Layer metadata
            return store.getLayerMetadata(layerName, key);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void putLayerMetadata(String layerName, String key, String value) {
        // ReadLock is used because we are changing the internal state of the store, not the MemoryBlobStore state
        readLock.lock();
        try {
            // Add a new Layer Metadata
            store.putLayerMetadata(layerName, key, value);
        } finally {
            readLock.unlock();
        }
    }

    /**
     * @return a {@link CacheStatistics} object containing the {@link CacheProvider} statistics
     */
    public CacheStatistics getCacheStatistics() {
        // ReadLock is used because we are changing the internal state of the cacheProvider, not the MemoryBlobStore state
        readLock.lock();
        try {
            return cacheProvider.getStatistics();
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Setter for the store to wrap
     * 
     * @param store
     */
    public void setStore(BlobStore store) {
        // WriteLock is used because we are changing the internal state of the MemoryBlobStore
        writeLock.lock();
        try {
            if (store == null) {
                throw new NullPointerException("Input BlobStore cannot be null");
            }
            this.store = store;
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * @return The wrapped {@link BlobStore} implementation
     */
    public BlobStore getStore() {
        // ReadLock is used because we are only returning the Store
        readLock.lock();
        try {
            return store;
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Setter for the cacheProvider to use
     * 
     * @param cacheProvider
     */
    public void setCacheProvider(CacheProvider cache) {
        // WriteLock is used because we are changing the internal state of the MemoryBlobStore
        writeLock.lock();
        try {
            if (cache == null) {
                throw new IllegalArgumentException("Input BlobStore cannot be null");
            }
            this.cacheProvider = cache;
        } finally {
            writeLock.unlock();
        }
    }

    /***
     * This method is used for converting a {@link TileObject} {@link Resource} into a {@link ByteArrayResource}.
     * 
     * @param obj
     * @return a TileObject with resource stored in a Byte Array
     * @throws StorageException
     */
    private TileObject getByteResourceTile(TileObject obj) throws StorageException {
        // Get TileObject resource
        Resource blob = obj.getBlob();
        final Resource finalBlob;
        // If it is a ByteArrayResource, the result is simply copied
        if (obj.getBlob() instanceof ByteArrayResource) {
            ByteArrayResource byteArrayResource = (ByteArrayResource) obj.getBlob();
            byte[] contents = byteArrayResource.getContents();
            byte[] copy = new byte[contents.length];
            System.arraycopy(contents, 0, copy, 0, contents.length);
            finalBlob = new ByteArrayResource(copy);
        } else {
            // Else the result is written to a new WritableByteChannel
            final ByteArrayOutputStream bOut = new ByteArrayOutputStream();
            WritableByteChannel wChannel = Channels.newChannel(bOut);
            try {
                blob.transferTo(wChannel);
            } catch (IOException e) {
                throw new StorageException(e.getLocalizedMessage(), e);

            }
            finalBlob = new ByteArrayResource(bOut.toByteArray());
        }
        // Creation of a new Resource
        TileObject cached = TileObject.createCompleteTileObject(obj.getLayerName(), obj.getXYZ(),
                obj.getGridSetId(), obj.getBlobFormat(), obj.getParameters(), finalBlob);
        return cached;
    }

    /**
     * {@link Callable} implementation used for creating various tasks to submit to the {@link MemoryBlobStore} executor service.
     * 
     * @author Nicola Lagomarsini GeoSolutions
     */
    static class BlobStoreTask implements Callable<Boolean> {

        /** Store on which tasks must be executed */
        private BlobStore store;

        /** Array of objects that must be used for the selected operation */
        private Object[] objs;

        /** Enum containing the kind of action to execute */
        private BlobStoreAction action;

        public BlobStoreTask(BlobStore store, BlobStoreAction action, Object... objs) {
            this.objs = objs;
            this.store = store;
            this.action = action;
        }

        @Override
        public Boolean call() throws Exception {
            boolean result = false;
            try {
                result = action.executeOperation(store, objs);
            } catch (StorageException s) {
                if (LOG.isErrorEnabled()) {
                    LOG.error(s.getMessage(), s);
                }
            }
            return result;
        }

    }

    /**
     * Enum containing all the possible operations that can be executed by a {@link BlobStoreTask}. Each operation must implement the
     * "executeOperation" method.
     * 
     * @author Nicola Lagomarsini GeoSolutions
     */
    public enum BlobStoreAction {
        PUT {
            @Override
            public boolean executeOperation(BlobStore store, Object... objs)
                    throws StorageException {
                if (objs == null || objs.length < 1 || !(objs[0] instanceof TileObject)) {
                    return false;
                }
                store.put((TileObject) objs[0]);
                return true;
            }
        },
        GET {
            @Override
            public boolean executeOperation(BlobStore store, Object... objs)
                    throws StorageException {
                if (objs == null || objs.length < 1 || !(objs[0] instanceof TileObject)) {
                    return false;
                }
                return store.get((TileObject) objs[0]);
            }
        },
        DELETE_SINGLE {
            @Override
            public boolean executeOperation(BlobStore store, Object... objs)
                    throws StorageException {
                if (objs == null || objs.length < 1 || !(objs[0] instanceof TileObject)) {
                    return false;
                }
                return store.delete((TileObject) objs[0]);
            }
        },
        DELETE_RANGE {
            @Override
            public boolean executeOperation(BlobStore store, Object... objs)
                    throws StorageException {
                if (objs == null || objs.length < 1 || !(objs[0] instanceof TileRange)) {
                    return false;
                }
                return store.delete((TileRange) objs[0]);
            }
        },
        DELETE_GRIDSET {
            @Override
            public boolean executeOperation(BlobStore store, Object... objs)
                    throws StorageException {
                if (objs == null || objs.length < 2 || !(objs[0] instanceof String)
                        || !(objs[1] instanceof String)) {
                    return false;
                }
                return store.deleteByGridsetId((String) objs[0], (String) objs[1]);
            }
        },
        DELETE_LAYER {
            @Override
            public boolean executeOperation(BlobStore store, Object... objs)
                    throws StorageException {
                if (objs == null || objs.length < 2 || !(objs[0] instanceof String)) {
                    return false;
                }
                return store.delete((String) objs[0]);
            }
        },
        CLEAR {
            @Override
            public boolean executeOperation(BlobStore store, Object... objs)
                    throws StorageException {
                store.clear();
                return true;
            }
        },
        DESTROY {
            @Override
            public boolean executeOperation(BlobStore store, Object... objs)
                    throws StorageException {
                store.destroy();
                return true;
            }
        },
        RENAME {
            @Override
            public boolean executeOperation(BlobStore store, Object... objs)
                    throws StorageException {
                if (objs == null || objs.length < 2 || !(objs[0] instanceof String)
                        || !(objs[1] instanceof String)) {
                    return false;
                }
                return store.rename((String) objs[0], (String) objs[1]);
            }
        };

        /**
         * Executes an operation defined by the Enum.
         * 
         * @param store
         * @param objs
         * @return operation result
         * @throws StorageException
         */
        public abstract boolean executeOperation(BlobStore store, Object... objs)
                throws StorageException;
    }
}
