# Freshworks_Assignment
A file based key-value data store that supports the basic CRD (create, read, and delete) operations. This data store is meant to be used as a local storage for one single process on one laptop. The data store can be exposed as a library to clients that can instantiate a class and work with the data store.


## Functionl Requirements

* It can be initialized using an optional file path. If one is not provided, it will reliably create itself in a reasonable location on the laptop.

* A new key-value pair can be added to the data store using the Create operation. The key is always a string- capped at 32chars. The value is always a JSON object- capped at 16KB.

* If Create is invoked for an existing key, an appropriate error is returned.

* A Read operation on a key can be performed by providing the key, and receiving the value in response, as a JSON object.

* A Delete operation can be performed by providing the key.

* Every key supports setting a Time-To-Live property when it is created. This property is optional. If provided, it will be evaluated as an integer defining the number of seconds the key must be retained in the data store. Once the Time-To-Live for a key has expired, the key will no longer be available for Read or Delete operations.

* Appropriate error responses are returned to a client if it uses the data store in unexpected ways or breaches any limits.

## Non-Functional Requirements

* The size of the file storing data will not exceed 1GB.

* More than one client process are not allowed to use the same file as a data store at any given time.

* A client process is allowed to access the data store using multiple threads, if it desires to. The data store is thread-safe.

* The client will bear as little memory costs as possible to use this data store, while deriving maximum performance with respect to response times for accessing the data store.
