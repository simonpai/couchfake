#couchfake

couchfake is a module that mocks Couchbase 2.0 Node.js API with simple in-memory JavaScript implementation. If you, for whatever reason, have trouble working with Couchbase 2.0 or with couchnode, you can still experience it with this module. For exmaple, 

* Having issue with dynamic IP
* Having problem compiling couchnode on Windows

## Difference

You still have to beware of the difference between couchfake and the real couchnode, and do NOT use it for production. In a summary, 

* couchfake does not do any optimization for MapReduce.
* couchfake does not have clustering and replica.
