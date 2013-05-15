#couchfake

couchfake is a module that mocks Couchbase 2.0 Node.js API with a simplest in-memory JavaScript implementation. If you, for whatever reason, have trouble working with Couchbase 2.0 with couchnode, for example,

* Running into the [dynamic IP issue](http://www.couchbase.com/issues/browse/MB-7398), or
* Having problem compiling couchnode module on Windows system,

you can play around this module as a placebo of the official couchnode.



## The Story

Roughly in March 2013 I was eager to try out Couchbase 2.0.0 on a Windows machine, and then I ran into the (infamous?) [dynamic IP issue](http://www.couchbase.com/issues/browse/MB-7398). The Couchbase team was very efficient and fixed the issue in a short time, but the fix was too late to catch the train of the upcoming 2.0.1. So, while waiting for the next Couchbase release, I tried to race against official 2.0.2 release with a mock couchbase implementation which passes all test cases coming from couchnode and here it is. :)

This is a project mostly for fun, and I don't guarantee the total correctness of this module. However I will seriously use it for development myself until Couchbase 2.0.2 is released.



## Project Status & Road Map

As of 2013/5/16:

* couchfake passed all 22 test cases from couchnode (with 1 skipped by default).
* couchfake runs a straightforward, uncached MapReduce algorithm.
* couchfake does not have any clustering and replica.
* you can override the core storaging methods of couchfake to achieve real persistence.

What I will probably do next:

* Refactoring
* Document the core storaging methods
* NPM
* May stop maintaining this project after Couchbase 2.0.2 is released and proven to be dynamic-IP-friendly.


