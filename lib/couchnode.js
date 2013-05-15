
// TOOD: hashkey

// constants //
var OPERATION_ADD = 1,
	OPERATION_REPLACE = 2,
	OPERATION_SET = 3,
	OPERATION_APPEND = 4,
	OPERATION_PREPEND = 5;

var errors = {
    success: 0x00,
    authContinue: 0x01,
    authError: 0x02,
    deltaBadVal: 0x03,
    objectTooBig: 0x04,
    serverBusy: 0x05,
    internal: 0x06,
    invalidArguement: 0x07,
    outOfMemory: 0x08,
    invalidRange: 0x09,
    genericError: 0x0a,
    temporaryError: 0x0b,
    keyAlreadyExists: 0x0c,
    keyNotFound: 0x0d,
    failedToOpenSymbol: 0x0e,
    failedToFindSymbol: 0x0f,
    networkError: 0x10,
    wrongServer: 0x11,
    notStored: 0x12,
    notSupported: 0x13,
    unknownCommand: 0x14,
    unknownHost: 0x15,
    protocolError: 0x16,
    timedOut: 0x17,
    connectError: 0x18,
    bucketNotFound: 0x19,
    clientOutOfMemory: 0x1a,
    clientTemporaryError: 0x1b,
    badHandle: 0x1c,
    serverBug: 0x1d
};

var observeStatus = {
    found: 0x00,
    persisted: 0x01,
    notFound: 0x80
};



// in-memory storage //
var _data = {},
	_locks = {}, 
	_ddocs = {};



// built-in reduce function //
function _count(key, values, rereduce) {
	if (rereduce) {
		var result = 0, len = values.length;
		for (var i = 0; i < len; i++) {
			result += values[i];
		}
		return result;
	} else {
		return values.length;
	}
}

function _sum(key, values, rereduce) {
	var sum = 0, len = values.length;
	for (var i = 0; i < len; i++) {
		sum = sum + values[i];
	}
	return(sum);
}

function _stat(key, values, rereduce) {
	// TODO
}

exports.builtInReduce = {
	count: _count,
	sum: _sum,
	stat: _stat
};



// engine definition //
var _cas = 0;
function new_cas() {
	return {
		str: '' + _cas++
	};
}

exports.define = function (definition) {
	if (!definition)
		return;
	for (f in definition) {
		if (_definition.fn[f])
			_definition.fn['$'+f] = _definition.fn[f];
		_definition.fn[f] = definition[f];
	}
}

function _definition(hosts, user, password, bucket) {
	this.hosts = hosts;
	this.uesr = user;
	this.password = password;
	this.bucket = bucket || 'default';
	this.init(hosts, user, password, bucket);
}

_definition.fn = _definition.prototype = {
	
	/** called when connection constrcuted
	 */
	init: function (hosts, user, password, bucket) {
		// ignore hosts and authorization
		this.dataBucket  = _data[bucket]  = _data[bucket] || {};
		this.locksBucket = _locks[bucket] = _locks[bucket] || {};
		this.ddocsBucket = _ddocs[bucket] = _ddocs[bucket] || {};
	},
	
	// fundamental access //
	getSync: function (ddoc, key) {
		return this[ddoc ? 'ddocsBucket' : 'dataBucket'][key];
	},
	
	setSync: function (ddoc, key, value) {
		this[ddoc ? 'ddocsBucket' : 'dataBucket'][key] = value;
	},
	
	removeSync: function (ddoc, key) {
		delete this[ddoc ? 'ddocsBucket' : 'dataBucket'][key];
	},
	
	forEach: function (callback) {
		var dataBucket = this.dataBucket; 
		for (var key in dataBucket)
			callback.call(dataBucket[key]);
	},
	
	// API //
	/** 
	 */
	get: function (key, callback) {
		callback(errors.success, key, this.getSync(false, key));
	},
	
	/** 
	 */
	getm: function (keys, callback) {
		var _this = this;
		keys.forEach(function (k) {
			_this.get(k, callback);
		});
	},
	
	/** 
	 */
	set: function (key, value, callback) {
		this.setSync(false, key, value);
		callback(errors.success);
	},
	
	/** 
	 */
	remove: function (key, callback) {
		this.removeSync(false, key);
		callback(errors.success);
	},
	
	/**
	 */
	setLock: function (key, lock) {
		if (lock)
			this.locksBucket[key] = 1;
		else
			delete this.locksBucket[key];
		return errors.success;
	},
	
	isLocked: function (key) {
		return this.locksBucket[key];
	},
	
	touch: function (key, expiry, callback) {
		// do nothing
		callback(errors.success);
	},
	
	view: function (docName, viewName, query, callback) { // TODO: move this out of definition
		var ddoc, view, map, mr;
		if (!(ddoc = this.ddocsBucket[docName])) {
			callback(errors.genericError, null);
			return;
		}
		if (!(view = ddoc.views[viewName])) {
			callback(errors.genericError, null);
			return;
		}
		if (!(map = view.map)) {
			callback(errors.genericError, null);
			return;
		}
		
		mr = new MapReducer(query, map, view.reduce);
		this.forEach(function () {
			mr.dispatch(this);
		});
		callback(errors.success, mr.results());
	},
	
	getDesignDoc: function (name, callback) {
		callback(errors.success, this.getSync(true, name));
	},
	
	setDesignDoc: function (name, doc, callback) {
		this.setSync(true, name, doc);
		callback(errors.success);
	},
	
	deleteDesignDoc: function (name, callback) {
		this.removeSync(true, name);
		callback(errors.success);
	},
	
	/*
	flush: function (callback) {
		this.dataBucket = _data[bucket] = {};
		// TODO: does flush include design docs?
		callback();
	},
	*/
	
	version: 'couchfake-0.0.1'
	
};

exports.CouchbaseImpl = function (hosts, user, password, bucket) {
	this.definition = new _definition(hosts, user, password, bucket);
	
	// when "conntection is done" we need to fire a "connect" event
	var _this = this;
	setTimeout(function () {
		_this.fire('connect');
	});
};

exports.CouchbaseImpl.prototype = {
	// events //
	_listeners: {},
	fire: function (event, data) {
		var list = this._listeners[event];
		if (list)
			for (var i = 0, len = list.length; i < len; i++)
				list[i](data);
	},
	
	// CouchbaseImpl API //
	on: function (event, callback) {
		var list = this._listeners[event];
		if (!list)
			list = this._listeners[event] = [];
		list.push(callback);
	},
	touch: function (key, expiry, handler, callback) {
		key = parse_key(key);
		function cb(err) {
			handler(callback, err || errors.success, key);
		}
		this.definition.touch(key, expiry, cb);
	},
	get: function (key, handler, callback) {
		key = parse_key(key);
		function cb(err, key, value) {
			var meta = (value && value.meta) || {};
			handler(callback, err || errors.success, key, meta.cas, meta.flags || 0, value && value.doc);
		}
		this.definition[Array.isArray(key) ? 'getm' : 'get'](key, cb);
	},
	getAndLock: function (key, exptime, handler, callback) {
		key = parse_key(key);
		function errcb(err) {
			if (err) 
				cb(err, key);
			return err;
		}
		function cb(err, key, value) {
			var meta = (value && value.meta) || {};
			handler(callback, err || errors.success, key, meta.cas, meta.flags || 0, value && value.doc);
		}
		
		var def = this.definition,
			isArray = Array.isArray(key),
			keyarr = isArray ? key : [key];
		
		for (var i = 0, len = keyarr.length; i < len; i++)
			if (errcb(def.isLocked(keyarr[i]) && errors.genericError)) return; // TODO: error code?
		
		// setTime first in case of any error
		setTimeout(function () {
			keyarr.forEach(function (k) {
				def.setLock(k, false);
			});
		}, 1000 * (exptime || 30));
		if (errcb(def.setLock(key, true))) return;
		
		def[isArray ? 'getm' : 'get'](key, cb);
	},
	unlock: function (key, handler, callback) {
		key = parse_key(key);
		var _this = this;
		if (Array.isArray(key)) {
			key.forEach(function (k) {
				_this.unlock(k, handler, callback);
			});
		} else {
			handler(callback, this.definition.setLock(key, false) || errors.success, key);
		}
	},
	store: function (op, key, doc, expiry, flags, cas, handler, callback) {
		key = parse_key(key);
		// TODO: may want to extract an errcb()
		function getcb(err, key, value) {
			if (err) {
				handler(callback, err, key, /*cas:*/0); // TODO: cas?
				return;
			}
		    switch (op) {
		    	case OPERATION_ADD:
		    		if (value != null)
		    			err = errors.keyAlreadyExists;
		    		break;
		    	case OPERATION_REPLACE:
		    	case OPERATION_APPEND:
		    	case OPERATION_PREPEND:
		    		if (value == null)
		    			err = errors.keyNotFound;
		    }
		    if (err) {
		    	handler(callback, err, key);
		    	return;
		    }
		    var rcas = value && value.meta.cas,
		    	scas;
		    // check CAS value
		    if (cas && cas !== rcas) {
		    	handler(callback, errors.genericError, key); // TODO: error code?
		    	return;
		    }
		    // check lock
		    if (!cas && def.isLocked(key)) {
		    	handler(callback, errors.genericError, key); // TODO: error code?
		    	return;
		    }
		    // acquire a new CAS value
		    scas = new_cas();
			
			// prepend, append doc
			switch (op) {
		    	case OPERATION_APPEND:
		    		doc = value.doc + doc;
		    		break;
		    	case OPERATION_PREPEND:
		    		doc = doc + value.doc;
			}
			
			def.set(key, {
				doc: doc, 
				// TODO: doesn't need to reconstruct meta
				// TODO: pull this out, create meta if new
				meta: { // TODO: need a docid?
					id: key, 
					type: 'json',  // TODO: other type
					expiration: expiry,
					flags: flags,
					cas: scas
				}
			}, function (err) {
				handler(callback, err || errors.success, key, scas);
			});
		}
		var def = this.definition;
		def.get(key, getcb);
	},
	arithmetic: function (key, offset, defaultValue, expiry, handler, callback) {
		key = parse_key(key);
		function getcb(err, key, value) {
			var v = value || {},
				d = v.doc,
				n = d != null ? (d + offset) : (defaultValue || 0),
				scas;
			// check lock
			if (def.isLocked(key)) {
				handler(callback, errors.genericError, key);
				return;
			}
			scas = new_cas();
			
			def.set(key, {
				doc: n,
				meta: { // TODO: pull this out, create meta if new
					id: key, 
					type: 'json',
					expiration: expiry,
					flags: 0,
					cas: scas
				}
			}, function (err) {
				handler(callback, err || errors.success, key, scas, n);
			});
		}
		var def = this.definition;
		def.get(key, getcb);
	},
	observe: function (key, handler, callback) {
		key = parse_key(key);
		function getcb(err, key, value) {
			if (!value) {
				handler(callback, errors.keyNotFound);
				return;
			}
			// data callback from master
			handler(callback, errors.success, key, value.meta.cas, 
				/*status*/observeStatus.persisted, /*from_master*/true, /*ttp*/0, /*ttr*/0);
			// terminator callback: leave key null
			handler(callback, errors.success);
		}
		var def = this.definition;
		def.get(key, getcb);
	},
	remove: function (key, cas, handler, callback) {
		key = parse_key(key);
		function getcb(err, key, value) {
			if (value == null)
				err = errors.keyNotFound;
			else if (cas && cas != value.meta.cas)
				err = errors.genericError; // TODO: error code?
			if (err) {
				handler(callback, err, key);
				return;
			}
			// check lock
			if (!cas && def.isLocked(key)) {
				handler(callback, errors.genericError, key);
				return;
			}
			def.remove(key, function (err) {
				handler(callback, err || errors.success, key, cas);
			});
		}
		var def = this.definition;
		def.get(key, getcb);
	},
	
	getVersion: function () {
		return this.definition.version;
	},
	strError: function (errorCode) {
		// return error string corresponding to errorCode
		if (errorCode === 0)
			return 'Success';
		return null; // TODO: I really have no idea about other error labels
	},
	
	view: function (docName, viewName, query, handler, callback) {
		function cb(err, results) {
			handler(callback, err || errors.success, results);
		}
		this.definition.view(docName, viewName, query, cb); // TODO: pull implementation here
	},
	setDesignDoc: function (name, doc, handler, callback) {
		function cb(err) {
			handler(callback, err || errors.success);
		}
		this.definition.setDesignDoc(name, doc, cb);
	},
	getDesignDoc: function (name, handler, callback) {
		function cb(err, value) {
			handler(callback, err || errors.success, value);
		}
		this.definition.getDesignDoc(name, cb);
	},
	deleteDesignDoc: function (name, handler, callback) {
		function cb(err) {
			handler(callback, err || errors.success);
		}
		this.definition.deleteDesignDoc(name, cb);
	},
	shutdown: function () {
		// do nothing
		// TODO: check spec
	}
};

function parse_key(key) {
	// key can be in the form of { key: 'xxx', hashkey: 'yyy' }
	// data with same hashkey is gauranteed to go to the same server,
	// so it does not matter here
	if (Array.isArray(key)) {
		var ks = [];
		for (var i = 0, len = key.length; i < len; i++)
			ks.push(parse_key(key[i]));
		return ks;
	}
	return key == null || typeof key === 'string' ? key : key.key;
}

function str_to_map_func(str) {
	return str_to_func('function (emit, doc, meta) { (' + str + ')(doc, meta); }');
}

function str_to_func(str) {
	var f;
	try {
		eval('f = ' + str + ';');
		if (typeof f === 'function')
			return f;
	} catch (e) {}
	return null;
}

function hash(arr) {
	// assume arr is an array of Strings
	var s = '', t;
	for (var i = 0, len = arr.length; i < len; i++) {
		t = arr[i];
		s += t == null ? '%$' : ('%#' + t.replace(/%/g, '%%'));
	}
	return s;
}

function MapReducer(query, map_func, reduce_func) {
	// map/reduce functions
	this.map_func = str_to_map_func(map_func);
	this.reduce_func = str_to_func(reduce_func);
	
	this._mapped = {};
	
	if (!map_func) {
		// TODO: throw exception
	}
	if (!query)
		return;
	
	//this.query = query || {}; // TODO: just grab entire query
	
	// filter
	this.startkey = query.startkey;
	this.startkey_docid = query.startkey_docid;
	this.endkey = query.endkey;
	this.endkey_docid = query.endkey_docid;
	this.inclusive_end = query.inclusive_end;
	
	this.key = query.key;
	this.keys = query.keys; // TODO: need to be array
	
	// grouping
	this.reduce = query.reduce;
	this.group = query.group;
	this.group_level = query.group_level;
	// TODO: check state
	
	// result ordering
	this.descending = query.descending;
	
	// result pagination
	this.skip = query.skip;
	this.limit = query.limit;
	
	// unused
	this.full_set = query.full_set;
	this.on_error = query.on_error;
	this.stale = query.stale;
	
}

MapReducer.prototype = {
	
	dispatch: function (v) {
		var doc = v.doc,
			meta = v.meta,
			_this = this;
		function em(key, value) {
			_this._emit(key, value, doc, meta);
		}
		this.map_func(em, doc, meta);
	},
	
	_emit: function (key, value, doc, meta) {
		
		// ignore keys other than key, keys if specified
		if (this.key && key != this.key)
			return;
		if (this.keys && this.keys.indexOf(key) < 0)
			return;
		
		var docid = meta.docid,
			end_bound = this.inclusive_end ? 1 : 0;
		
		var dir = this.descending ? -1 : 1;
		
		// ignore anything less than startkey, startkey_docid
		if (this.startkey && (dir * compare(key, this.startkey) < 0))
			return;
		if (this.startkey_docid && (dir * compare(docid, this.startkey_docid) < 0))
			return;
		
		// ignore anything larger than endkey, endkey_docid
		if (this.endkey && (dir * compare(key, this.endkey) >= end_bound))
			return;
		if (this.endkey_docid && (dir * compare(docid, this.endkey_docid) >= end_bound))
			return;
		
		if (this.reduce) { // TODO: check spec, may depends on designDoc reduce availability as well
			var level = this.group_level,
				group_key = 
					level ? key.slice(0, level) : // group to specified level
					(this.group || level == 0) ? null : // group to level 0
					key; // group to last level
			
			_get_slot(group_key).values.push(value);
			
		} else {
			// no reduce, split down to doc level
			this._mapped[hash([key, docid])] = { key: key, value: value, id: docid, doc: doc };
		}
		
	},
	
	_get_slot: function (key) {
		var hashed = hash(key),
			slot = this._mapped[hashed];
		if (!slot)
			this._mapped[hashed] = slot = { key: key, values: [] };
		return slot;
	},
	
	results: function () {
		var rows = this._rows, 
			skip = this.skip, 
			limit = this.limit,
			reduce = this.reduce,
			reduce_func = this.reduce_func,
			mapped, keys;
		if (!rows) {
			mapped = this._mapped;
			keys = Object.keys(mapped).sort(this.descending ? compare_desc : compare);
			
			var start = skip || 0,
				end = limit ? Math.min(start + limit, keys.length) : keys.length;
			this._rows = rows = [];
			
			for (var i = start, v; i < end; i++) {
				v = mapped[keys[i]];
				if (reduce) {
					// if reduced, only need to include key and value
					rows.push({ key: v.key, value: reduce_func(v.key, v.values, false) });
				} else {
					// otherwise, also include id and doc
					rows.push(v);
				}
			}
		}
		return rows;
	}
};

function compare_desc(a, b) {
	return compare(b, a);
}

// http://www.couchbase.com/docs/couchbase-manual-2.0/couchbase-views-writing-querying-ordering.html
function compare(a, b) {
	var ta = comparison_type(a),
		tb = comparison_type(b);
	if (ta < tb)
		return -1;
	else if (ta > tb)
		return 1;
	else {
		switch (ta) {
			case 3: // number
			case 4: // string
				return a < b ? -1 : a == b ? 0 : 1; // TODO: fix string comparison
			case 5: // array
				var alen = a.length,
					blen = b.length,
					minlen = Math.min(alen, blen);
				for (var i = 0, c; i < minlen; i++) {
					c = compare(a[i], b[i]);
					if (c != 0)
						return c;
				}
				return alen < blen ? -1 : alen == blen ? 0 : 1;
			case 6: // object
				var akeys = Object.keys(a).sort(),
					bkeys = Object.keys(b).sort(),
					ak, bk, av, bv, c;
				while (ak = akeys.shift() & bk = bkeys.shift()) {
					c = compare(ak, bk);
					if (c != 0)
						return c;
					c = compare(av, bv);
					if (c != 0)
						return c;
				}
				return 0;
			default: // 0-2, 7
				return 0;
		}
	}
}

// TODO: check: does couchbase spec have no problem with Date?
function comparison_type(a) {
	if (a === null || a === undefined)
		return 0;
	var type = typeof a;
	switch (type) {
		case 'boolean':
			return a ? 2 : 1; // false: 1, true: 2
		case 'number':
			return 3;
		case 'string':
			return 4;
		case 'object':
			return (a instanceof Array) ? 5 : 6; // array: 5, object: 6
		default:
			return 7;
	}
}

function clone(a) {
	if (a == null)
		return null;
	var b = {};
	for (var p in a)
		if (a.hasOwnProperty(p))
			b[p] = a[p];
	return b;
}
