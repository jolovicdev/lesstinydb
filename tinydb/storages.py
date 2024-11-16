"""
Contains the :class:`base class <tinydb.storages.Storage>` for storages and
implementations.
"""

from contextlib import contextmanager
import fcntl
import io
import json
import os
import warnings
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional

__all__ = ('Storage', 'JSONStorage', 'MemoryStorage')


def touch(path: str, create_dirs: bool):
    """
    Create a file if it doesn't exist yet.

    :param path: The file to create.
    :param create_dirs: Whether to create all missing parent directories.
    """
    if create_dirs:
        base_dir = os.path.dirname(path)

        # Check if we need to create missing parent directories
        if not os.path.exists(base_dir):
            os.makedirs(base_dir)

    # Create the file by opening it in 'a' mode which creates the file if it
    # does not exist yet but does not modify its contents
    with open(path, 'a'):
        pass


class Storage(ABC):
    """
    The abstract base class for all Storages with transaction support.
    A Storage (de)serializes the current state of the database and stores it in
    some place (memory, file on disk, ...).
    """
    def __init__(self):
        self._in_transaction = False

    @abstractmethod
    def read(self) -> Optional[Dict[str, Dict[str, Any]]]:
        """Read the current state."""
        raise NotImplementedError('To be overridden!')

    @abstractmethod
    def write(self, data: Dict[str, Dict[str, Any]]) -> None:
        """Write the current state of the database to the storage."""
        raise NotImplementedError('To be overridden!')

    def close(self) -> None:
        """Optional: Close open file handles, etc."""
        pass

    @abstractmethod
    def _begin_transaction(self) -> None:
        """
        Implementation-specific transaction start logic.
        Should handle creating backups, acquiring locks, etc.
        """
        raise NotImplementedError('To be overridden!')

    @abstractmethod
    def _commit_transaction(self) -> None:
        """
        Implementation-specific transaction commit logic.
        Should handle cleanup, releasing locks, etc.
        """
        raise NotImplementedError('To be overridden!')

    @abstractmethod
    def _rollback_transaction(self) -> None:
        """
        Implementation-specific transaction rollback logic.
        Should handle restoring from backup, cleanup, releasing locks, etc.
        """
        raise NotImplementedError('To be overridden!')

    def begin(self) -> None:
        """Begin a new transaction."""
        if self._in_transaction:
            raise RuntimeError("Transaction already in progress")
        self._in_transaction = True
        self._begin_transaction()

    def commit(self) -> None:
        """Commit the current transaction."""
        if not self._in_transaction:
            raise RuntimeError("No transaction in progress")
        try:
            self._commit_transaction()
        finally:
            self._in_transaction = False

    def rollback(self) -> None:
        """Rollback the current transaction."""
        if not self._in_transaction:
            raise RuntimeError("No transaction in progress")
        try:
            self._rollback_transaction()
        finally:
            self._in_transaction = False

    @contextmanager
    def transaction(self):
        """Context manager for transaction handling."""
        self.begin()
        try:
            yield self
            self.commit()
        except:
            self.rollback()
            raise

    def __enter__(self):
        """Support for context manager protocol."""
        return self.transaction().__enter__()

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Handle transaction completion in context manager."""
        return self.transaction().__exit__(exc_type, exc_val, exc_tb)


class JSONStorage(Storage):
    """Store the data in a JSON file with proper transaction support."""
    def __init__(self, path: str, create_dirs=False, encoding=None, access_mode='r+', **kwargs):
        super().__init__()
        self._mode = access_mode
        self.path = path
        self.encoding = encoding or 'utf-8'  # Make sure we have an encoding
        self.kwargs = kwargs
        self._lock_file = None
        if any([character in self._mode for character in ('+', 'w', 'a')]):
            touch(path, create_dirs=create_dirs)
            
        self._handle = open(path, mode=self._mode, encoding=self.encoding)

    def _acquire_lock(self):
        """Acquire an exclusive lock on the database file"""
        if not self._lock_file:
            lock_path = f"{self.path}.lock"
            self._lock_file = open(lock_path, 'w')
        fcntl.flock(self._lock_file.fileno(), fcntl.LOCK_EX)

    def _release_lock(self):
        """Release the exclusive lock on the database file"""
        fcntl.flock(self._lock_file.fileno(), fcntl.LOCK_UN)

    def _begin_transaction(self) -> None:
        """Implementation of transaction start for JSON storage."""
        self._backup_path = f"{self.path}.backup"
        self._acquire_lock()
        # Create backup file - using same encoding as main file
        with open(self.path, 'r', encoding=self.encoding) as source:
            content = source.read()
            with open(self._backup_path, 'w', encoding=self.encoding) as target:
                target.write(content)

    def _commit_transaction(self) -> None:
        """Implementation of transaction commit for JSON storage."""
        self._handle.flush()
        os.fsync(self._handle.fileno())
        # Remove backup after successful commit
        if os.path.exists(self._backup_path):
            os.remove(self._backup_path)
        self._release_lock()

    def _rollback_transaction(self) -> None:
        """Implementation of transaction rollback for JSON storage."""
        try:
            # Close current handle
            self._handle.close()
            
            # Restore from backup
            with open(self._backup_path, 'r', encoding=self.encoding) as backup:
                content = backup.read()
            
            # Write backup content to main file
            with open(self.path, 'w', encoding=self.encoding) as main:
                main.write(content)
            
            # Reopen the handle
            self._handle = open(self.path, mode=self._mode, encoding=self.encoding)
            
        finally:
            # Clean up backup
            if os.path.exists(self._backup_path):
                os.remove(self._backup_path)
            self._release_lock()


    def close(self) -> None:
        """Close the file handle."""
        self._handle.close()

    def read(self) -> Optional[Dict[str, Dict[str, Any]]]:
        # Get the file size
        self._handle.seek(0, os.SEEK_END)
        size = self._handle.tell()

        if not size:
            return None
        
        # Return to start and read
        self._handle.seek(0)
        return json.load(self._handle)

    def write(self, data: Dict[str, Dict[str, Any]]) -> None:
        # Move to start
        self._handle.seek(0)

        # Write the serialized data
        serialized = json.dumps(data, **self.kwargs)
        
        try:
            self._handle.write(serialized)
        except io.UnsupportedOperation:
            raise IOError(f'Cannot write to the database. Access mode is "{self._mode}"')

        # Ensure written to disk
        self._handle.flush()
        os.fsync(self._handle.fileno())

        # Truncate any remaining data
        self._handle.truncate()

class MemoryStorage(Storage):
    """Store the data in memory with transaction support."""
    def __init__(self):
        super().__init__()
        self.memory = None
        self._backup = None

    def read(self) -> Optional[Dict[str, Dict[str, Any]]]:
        return self.memory

    def write(self, data: Dict[str, Dict[str, Any]]):
        self.memory = data

    def _begin_transaction(self) -> None:
        """Implementation of transaction start for memory storage."""
        self._backup = json.loads(json.dumps(self.memory)) if self.memory is not None else None

    def _commit_transaction(self) -> None:
        """Implementation of transaction commit for memory storage."""
        self._backup = None

    def _rollback_transaction(self) -> None:
        """Implementation of transaction rollback for memory storage."""
        self.memory = self._backup
        self._backup = None