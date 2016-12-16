/**
 * Created by ys2n on 7/18/14.
 */

var solr = require('solr-client');
var async = require('async');
var log = require('tracer').colorConsole({level: 'warn'});

var asset_index_options = {
    'host': 'ss558499-us-east-1-aws.measuredsearch.com',
    'port': 443,
    'secure': true,
    'path': '/solr',
    'core': 'kmassets_dev'
};

//var asset_index_options = {
//    'host': 'drupal-index.shanti.virginia.edu',
//    'port': 80,
//    'path': '/solr-test',
//    'core': '/kmindex'
//};

var term_index_options = {
    'host': 'ss558499-us-east-1-aws.measuredsearch.com',
    'port': 443,
    'secure': true,
    'path': '/solr',
    'core': 'kmterms_dev'
};

//var term_index_options = {
//    'host': 'drupal-index.shanti.virginia.edu',
//    'port': 80,
//    'path': '/solr-test',
//    'core': '/kmterms'
//};

var asset_client = solr.createClient(asset_index_options);
asset_client.basicAuth("solradmin","");  // TODO: REFACTOR! CENTRALIZE BASIC AUTH

var term_client = solr.createClient(term_index_options);
term_client.basicAuth("solradmin","");  // TODO: REFACTOR! CENTRALIZE BASIC AUTH


exports.term_index_options = term_index_options;
exports.asset_index_options = asset_index_options;

exports.addDocs = function (docs, user_pwd_auth, callback) {
    asset_client.autoCommit = true;
    if (!_.isEmpty(user_pwd_auth)) {
        asset_client.basicAuth(user_pwd_auth);
    }
    asset_client.add(docs, function (err, report) {
        // log.info("%j",docs);
        if (err) {
            console.log(err);
        } else {
            console.log(report);
        }
        callback(err, report);
    });

};

exports.removeDoc = function (uid, user_pwd_auth, callback) {
    if (!_.isEmpty(user_pwd_auth)) {
        asset_client.basicAuth(user_pwd_auth);
    }
    console.log("removeDoc called with uid = " + uid + " and callback = " + callback);
    asset_client.autoCommit = false;
    asset_client.delete("uid", uid, function (err, x) {
        if (err) {
            log.info("%j", err);
            asset_client.rollback(err, function () {
            });   // anything to handle after a rollback?
        } else {
            log.info("%j", x);
            asset_client.commit();
        }
    });

    if (callback) {
        callback(null, null);
    }
};

exports.lastUpdated = function (solrclient, uid, callback) {
    var query = solrclient.createQuery().q("uid:" + uid);

    solrclient.search(query, function (err, obj) {
        if (err) {

            console.log("lastUpdated() Error using solrclient: " + JSON.stringify(solrclient.options));
            log.info("%j", err);
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
};

exports.assetLastUpdated = function (uid, callback) {
    exports.lastUpdated(asset_client, uid, callback);
};

exports.termLastUpdated = function (uid, callback) {
    exports.lastUpdated(term_client, uid, callback);
};

exports.getAssetEtag = function (uid, callback) {
    var query = asset_client.createQuery().q("uid:" + uid).fl("etag");

    asset_client.search(query, function (err, obj) {
        if (err) {
            console.log("getAssetEtag() Error:");
            log.info("%j", err);
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
};

exports.getTermEtag = function (kid, callback) {
    const RETRY_LIMIT = 3;
    var query = term_client.createQuery().q("uid:" + kid).fl("etag,_version_i");


    //   SOMETHING IS WRONG HERE...   WHAT IS IT?
    //   WHAT CONSTITUTES SUCCESS, and WHAT CONSTITUTES FAILURE HERE?

    async.retry(RETRY_LIMIT, function (callback, results) {
        // log.log("Trying... "  + "kid = " + kid);
        term_client.search(query, function (err, obj) {
            if (err) {
                log.log("getTermEtag() Error for kid = " + kid + ":");
                log.info("%j", err);
                callback(err);
            } else {
                // log.log("getTermEtag(): " + JSON.stringify(obj,undefined,2));
                if (obj.response.numFound == 0) {
                    log.log("calling back null,null ");
                    callback(null, null);
                } else if (obj.response.docs[0].etag) {
                    callback(null, {'etag': obj.response.docs[0].etag, 'version': obj.response.docs[0]._version_i});
                }
            }

        });
    }, function (err, result) {
        callback(err, result);
    });
};


exports.getTermCheckSum = function (uid, callback) {
    try {
        var query = term_client.createQuery().q("id:" + uid).fl("checksum");


        term_client.search(query, function (err, obj) {
            if (err) {
                log.log("getTermCheckSum() Error:");
                log.info("%j", err);
                callback(err, null);
            } else {
                // log.log("getTermCheckSum(): " + JSON.stringify(obj,undefined,2));
                if (obj.response.numFound == 0) {
                    log.log("calling back null,null ");
                    callback(null, null);
                } else if (obj.response.docs[0].checksum) {
                    callback(null, obj.response.docs[0].checksum);
                } else {
                    callback(null, null);
                }
            }
        });
    } catch (e) {
        log.log(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> ERROR >>>>>>>>>>>>>>>>>>>>>");
        callback(e);
    }
};


exports.addTerms = function (terms, callback, commit) {
    commit = commit || true;
    term_client.autoCommit = true;
    log.debug("adding to " + JSON.stringify(term_index_options));
    term_client.add(terms, function (err, report) {
        if (err) {
            log.log(err);
        } else {
            // log.log(report);
            if (commit) {
                term_client.commit();
            }
        }
        callback(err, report);
    });
};

exports.getAssetDocs = function (service, callback) {
    //log.error("service = " + service);
    var query = asset_client.createQuery().q({"service": service}).fl("id,service").rows(30000);
    asset_client.search(query, function (err, obj) {
        if (err) {
            log.log("getAssetDocs() Error:");
            log.info("%j", err);
            callback(err);
        } else {
            // log.log("getAssetDocs(): " + JSON.stringify(obj,undefined,2));
            callback(null, obj.response.docs);
        }
    });
};

