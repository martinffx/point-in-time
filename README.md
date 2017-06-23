# Point in Time database

This is a failed attempt to use `SQLAlchemy` to write a point in time data store.
It was extracted from another project as unfortunately it seems to lock the
database. I'll need to find a way to control how the transactions are performed.
I've seen this method work at scale using stored procedures.
