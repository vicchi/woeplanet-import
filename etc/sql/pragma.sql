PRAGMA synchronous=0;
PRAGMA locking_mode=EXCLUSIVE;
PRAGMA journal_mode=DELETE;
--PRAGMA journal_mode=WAL;
PRAGMA main.page_size=4096;
PRAGMA main.cache_size=10000;
