/**
 * Created by ys2n on 7/18/14.
 */

var solr = require('solr-client');
var async = require('async');

var asset_index_options = {
    'host': 'kidx.shanti.virginia.edu',
    'port': 80,
    'path': '/solr',
    'core': 'kmindex'
}

//var asset_index_options = {
//    'host': 'drupal-index.shanti.virginia.edu',
//    'port': 80,
//    'path': '/solr-test',
//    'core': '/kmindex'
//};

var term_index_options = {
    'host': 'kidx.shanti.virginia.edu',
    'port': 80,
    'path': '/solr',
    'core': 'termindex'
}

//var term_index_options = {
//    'host': 'drupal-index.shanti.virginia.edu',
//    'port': 80,
//    'path': '/solr-test',
//    'core': '/kmterms'
//};

var asset_client = solr.createClient(asset_index_options);
var term_client = solr.createClient(term_index_options);




exports.term_index_options = term_index_options;
exports.asset_index_options = asset_index_options;

exports.addDocs = function (docs, callback) {
    asset_client.autoCommit = true;
    asset_client.add(docs, function (err, report) {
        // console.dir(docs);
        if (err) {
            console.log(err);
        } else {
            console.log(report);
        }
        callback(err, report);
    });

}

exports.removeDoc = function (uid, callback) {
    console.log("removeDoc called with uid = " + uid + " and callback = " + callback);
    asset_client.autoCommit=false;
    asset_client.delete("uid",uid, function(err,x) {
        // console.log("CALLBACK TO removeDoc");
        console.dir("err: " + err);
        console.dir("doc: " + x);
    });
    asset_client.commit();
    if (callback) {
        callback(null,null);
    }
}

exports.lastUpdated = function (solrclient, uid, callback) {
    var query = solrclient.createQuery().q("uid:" + uid)

    solrclient.search(query, function (err, obj) {
        if (err) {
            console.log("lastUpdated() Error using solrclient: " + JSON.stringify(solrclient.options));
            console.dir(err);
        } else {
            console.log("assetLastUpdated(): " + JSON.stringify(obj, undefined, 2));
            if (obj.response.numFound == 0) {
                console.log("calling back null,0");
                callback(null, 0);
            } else if (obj.response.docs[0].timestamp) {
                callback(null, new Date(obj.response.docs[0].timestamp).getTime());
            } else if (obj.response.docs[0]["_timestamp_"]) {
                callback(null, new Date(obj.response.docs[0]["_timestamp_"]).getTime())
            }

        }
    });
}

exports.assetLastUpdated = function (uid, callback) {
    exports.lastUpdated(asset_client,uid, callback);
}

exports.termLastUpdated = function (uid, callback) {
    exports.lastUpdated(term_client, uid, callback);
}

exports.getAssetEtag = function (uid, callback) {
    var query = asset_client.createQuery().q("uid:" + uid).fl("etag");

    asset_client.search(query, function (err, obj) {
        if (err) {
            console.log("getAssetEtag() Error:");
            console.dir(err);
        } else {
            // console.log("getAssetEtag(): " + JSON.stringify(obj,undefined,2));
            if (obj.response.numFound == 0) {
                // console.log("calling back null,null to " + callback);
                callback(null, null);
            } else if (obj.response.docs[0].etag) {
                callback(null, obj.response.docs[0].etag);
            }
        }
    });
}

exports.getTermEtag = function (kid, callback) {
    const RETRY_LIMIT = 3;
    var query =  term_client.createQuery().q("uid:" + kid).fl("etag,_version_i");


    //   SOMETHING IS WRONG HERE...   WHAT IS IT?
    //   WHAT CONSTITUTES SUCCESS, and WHAT CONSTITUTES FAILURE HERE?

    async.retry(RETRY_LIMIT, function (callback, results) {
        // console.log("Trying... "  + "kid = " + kid);
        term_client.search(query, function (err, obj) {
            if (err) {
                console.log("getTermEtag() Error for kid = " + kid + ":");
                console.dir(err);
                callback(err);
            } else {
                // console.log("getTermEtag(): " + JSON.stringify(obj,undefined,2));
                if (obj.response.numFound == 0) {
                    console.log("calling back null,null ");
                    callback(null, null);
                } else if (obj.response.docs[0].etag) {
                    callback(null, {'etag':obj.response.docs[0].etag, 'version':obj.response.docs[0]._version_i });
                }
            }

        });
    },  function (err, result) {
        callback(err,result);
    });
}


exports.getTermCheckSum = function (uid, callback) {
    try {
        var query = term_client.createQuery().q("id:" + uid).fl("checksum");


        term_client.search(query, function (err, obj) {
            if (err) {
                console.log("getTermCheckSum() Error:");
                console.dir(err);
                callback(err, null);
            } else {
                // console.log("getTermCheckSum(): " + JSON.stringify(obj,undefined,2));
                if (obj.response.numFound == 0) {
                    console.log("calling back null,null ");
                    callback(null, null);
                } else if (obj.response.docs[0].checksum) {
                    callback(null, obj.response.docs[0].checksum);
                } else {
                    callback(null, null);
                }
            }
        });
    } catch (e) {
        console.log(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> ERROR >>>>>>>>>>>>>>>>>>>>>");
        callback(e);
    }
}


exports.addTerms = function (terms, callback, commit) {
    commit = commit || true;
    term_client.autoCommit = true;
    console.log("adding to " + JSON.stringify(term_index_options));
    term_client.add(terms, function (err, report) {
        if (err) {
            console.log(err);
        } else {
            // console.log(report);
		if (commit) {
			term_client.commit();
		}
        }
        callback(err, report);
    });
}

exports.getAssetDocs = function (service, callback) {
    //console.error("service = " + service);
     var query = asset_client.createQuery().q({"service":service}).fl("id,service").rows(30000);
    asset_client.search(query, function (err, obj) {
        if (err) {
            console.log("getAssetDocs() Error:");
            console.dir(err);
            callback(err);
        } else {
            // console.log("getAssetDocs(): " + JSON.stringify(obj,undefined,2));
            callback(null, obj.response.docs);
        }
    });
}

