/**
 * Created by ys2n on 7/16/14.
 *
 * This will be the set of functions needed to populate the index
 *
 * Will need to populate the index from multiple sources
 * Will need to handle multiple kmapid namespaces
 * Will need to queue and batch indexing jobs as needed.
 *
 */

var mb = require("../connectors/mediabase");
var ss = require("../connectors/sharedshelf");
var km = require("../connectors/kmaps");
var async = require("async");
var sm = require("../connectors/solr_manager");
var domain = require("domain");
var _ = require("underscore");
var crypto = require('crypto');




exports.documentStale = function (documentUid, staleTime, callback) {
    var now = new Date().getTime();
    sm.assetLastUpdated(documentUid, function(err, timestamp) {

        console.log("documentStale:")
//        console.log("   now       = " + now);
//        console.log("   timestamp = " + timestamp);
        console.log("   staleTime = " + staleTime);
        console.log("   diff      = " + (now-timestamp));
        console.log("   stale  = " + ((now - timestamp) > staleTime))


        if ((now - timestamp) > staleTime) {
            // console.log("documentStale calling back false to " + callback );
            callback(null,false);
        } else {
            // console.log("documentStale calling back true" );
            callback(null,true);
        }
    });
}

exports.getDocumentFromConnectorsByKMapId = function (kmapid, connectorlist, callback) {

    async.parallel(
        connectorlist,
        function(err,res) {
            callback(err,_.flatten(res))
        });

}

exports.getDocumentsByKMapId = function(kmapid, callback) {
    var connlist =
        [
            function(cb) {
                mb.getDocumentsByKmapId(kmapid,cb);
            },
            function(cb) {
                ss.getDocumentsByKmapId(kmapid,cb);
            }
        ];

    exports.getDocumentFromConnectorsByKMapId(kmapid, connlist, callback);

};

exports.getDocumentsByKMapIdStale = function(kmapid, staletime, callback) {
    var connlist =
        [
            function(cb) {
                mb.getDocumentsByKmapIdStale(kmapid, staletime, cb);
            },
            function(cb) {
                ss.getDocumentsByKmapIdStale(kmapid, staletime, cb);
            }
        ];

    exports.getDocumentFromConnectorsByKMapId(kmapid, connlist, callback);

};


exports.populateIndexByKMapId = function(kmapid, callback) {
    throw new Error("DO NOT USE:  for now.");
//    console.trace("calling getDocumentsByKmapId with kmapid = " + kmapid);
    exports.getDocumentsByKMapId(kmapid,
            function (err, docs) {
                sm.addAssets(docs,callback);
                // console.log("ADDED: " + JSON.stringify(docs,undefined,2));
            }
    )
};

exports.populateIndexByKMapIdStale = function(kmapid, staletime, callback) {
    exports.getDocumentsByKMapIdStale(kmapid,
        staletime,
        function (err, docs) {
            sm.addAssets(docs, callback);
            // console.log("ADDED: " + JSON.stringify(docs,undefined,2));
        }
    )
};

exports.populateIndexByServiceIdStale = function (serviceConnector, id, staletime, callback) {
    exports.documentStale(serviceConnector.getUid(id), function(err, fresh) {
        if (fresh) {
            // short-circuit --  may need to mock up a response!
            callback(null,{});
        } else {
            // delegate
            exports.populateIndexByServiceId(serviceConnector, id, callback);
        }
    });

};

exports.populateIndexByServiceId = function (serviceConnector, id, callback) {

    serviceConnector.getDocument(id, function(err, doc) {
        if (err) {

            //
            //   TODO: HANDLE MISSING RESOURCE HERE?  CORRECT THIS LOGIC WHEN THE RETURN CHANGES
            if (err.message == "Unexpected end of input") {
                // THIS LOGIC IS PROBABLY NO LONGER APPLICABLE
                console.error("HOLY MOLY ROLY POLY! : " + err.message);
                sm.removeDoc(id);
            } else {
                if(err.status === 404) {
                    var uid = err.uid;
                    var action = err.action;
                    console.log("ERROR: " + JSON.stringify(err, undefined, 2));
                }
                sm.removeDoc(uid, callback);
            }
    } else {
            console.log("ADDING TO SOLR");
            // console.dir(doc);
            sm.addAssets([doc], callback);
        }
    });
};

exports.rangePopulateIndexByService = function (serviceConnector, start, finish, callback) {
    throw new Error("DO NOT USE: for now");
    async.concatSeries(_.range(start,finish+1), function(id,cb){
        exports.populateIndexByServiceId(serviceConnector, id, function( err, ret) {
            if (err) {
                console.log("ERRORSY BERRORS: " + JSON.stringify(err));
                console.log("SERVICE CONNECTOR: " + serviceConnector);
                console.log("There was an error for id = " + id + ".   Ignoring and returning null" );
                cb(null,null);
            } else {
                cb(null,ret);
            }
        });
    },
    function(err,ret) {

        // Shouldn't ever see an err
        console.log("2Err = " + err);
        console.log("2Ret = " + JSON.stringify(ret));
        callback(err,ret);
    });
};

exports.populateTermIndex = function(host, master_callback) {

    const LIST_LIMIT = 0; // set to non-zero for testing ONLY
    const CONCURRENCY = 5;

    var dom = domain.create();
    dom.on('error', function(er) {
        console.trace("UnCaught exception!");
        console.error(er.stack);
    });

    dom.run(


        function() {
        km.getKmapsList(host, function (err, list) {
            console.log("Err = " + err);
	    var coin = Math.floor(Math.random() * 2)
            if (coin === 1) { 
		list = list.reverse();
	    }

            const JUMP = Math.floor(Math.random() * list.length);

            // truncating filter useful for testing.
            if (LIST_LIMIT) {
                list = _.first(list, LIST_LIMIT);
            }

            console.log("LIST length before = " + list.length);


            list.sort(function(a, b){return a-b});
            // list = _.unique(list,false);


            if (JUMP) {
                var last = _.last(list, (list.length - JUMP));
                var first = _.first(list, (JUMP));
                list = _.union(last,first);
            }

            console.log("LIST length after = " + list.length);


            async.mapLimit(list, CONCURRENCY, function iterator(kid, callback) {

//                console.log("host = " + host);
                    var ord = (_.indexOf(list, kid, false) + 1) + "/" + (list.length);
                    console.log("======= (" + ord + ") kid = " + kid  + " ========");

/////  ARGH THIS IS JUST WRONG!  REFACTOR THIS SUCKER! /////////
                    if (host.indexOf("subjects") > -1) {
                        kid = "subjects-" + kid;
                    } else {
                        kid = "places-" + kid;
                    }
/////////////////////////////////////////////////////////
//                 console.log("iterate: " + kid);
                    //  DO CHECKSUM or ETAGS check here


                    km.getKmapsDocument(kid, function (err, doc) {
                        if (err) {
                            console.log("Error retrieving " + kid);
                            callback(null, {"ignored error": err});
                        }
                        // console.log("Checking: (" + ord + ")");
                        if (doc !== null) {

                            // console.error("HERES THE DOC " + ord + ":" + JSON.stringify(doc,undefined, 2));

                            var ck = doc.checksum;
                            var etag = doc.etag;
                            var version = + doc._version_i;

                            // console.log("Checking ETAG (" + ord + ")");
                            sm.getTermEtag(doc.id, function (err, recorded_etag) {

                                if (err) {
                                    console.trace("Calling back err from getKmapsDocument/getTermsEtag callback.");
                                    callback(err);
                                    return;
                                }

                                if (recorded_etag === null) {
                                    recorded_etag = { etag: "", version:0 };
                                }


                                if (etag !== recorded_etag.etag || recorded_etag.version !== km.getVersion()) {
                                    console.log("    ETAG: " + etag);
                                    console.log("Rec ETAG: " + recorded_etag.etag);
                                    console.log("VERSION: " + km.getVersion());
                                    console.log("REC VERSION: " + recorded_etag.version);
                                    console.log("Checking TermCheckSum (" + ord + ")");
                                    sm.getTermCheckSum(doc.id, function (err, recorded_ck) {

                                        if (err) {

                                            console.trace("Calling back err from getKmapsDocument/getTermsEtag/getTermCheckSum callback.");
                                            callback(err);
                                            return;
                                        };

                                        console.log("CHECKSUMS: " + ck + " ::: " + recorded_ck);
					var force_commit = ( Math.floor(Math.random() * 10) == 0 );
                                        if (ck != recorded_ck) {
                                            //console.log("writing: " + JSON.stringify(doc, undefined, 2));
                                             console.log("writing: (" + ord + ")");
                                            sm.addTerms([doc], function (err, response) {
                                                if (err) {
                                                    console.trace("Calling back err from getKmapsDocument/getTermsEtag/getTermCheckSum/sm.addTerms callback.");
                                                }
                                                // console.dir(response);
                                                callback(err, response);
                                            }, force_commit);
                                        } else {
                                            // console.log("skipping...  checksums match: " + ck + " === " + recorded_ck);
                                            callback(null, {"skipping": "checksums match"})
                                        }
                                    });
                                } else {
                                    callback(null, {"skipping": "ETAGs match"});
                                }
                            });
                        }
                    });
                    //
                },
                function final(err, results) {
                    console.log("final: done!");
                    master_callback(err, results);
                });
        });
    });

    // + differentiate subject and places in the index!
    // + Use updated_at for freshness.
    // + see about managed schema https://cwiki.apache.org/confluence/display/solr/Managed+Schema+Definition+in+SolrConfig
    // + consider locking down schema after it settles down
    // + learn about copyfield for index searches
    // + need to handle transient errors



}

exports.updateEntries = function(serviceName, serviceConnector, master_callback) {

    sm.getAssetDocs(serviceName, function(err, docs) {

        docs.forEach( function(x) {
             console.dir (x);

                serviceConnector.getDocument(x.id, function (a,b) {
                    console.log ("a");
                    console.dir(a);
                    console.dir(b);


                })

        })


        master_callback(null, { nothing: "doing"});

    });






}

exports.getTermCheckSum = function(id,callback) {
    // delegate
    sm.getTermCheckSum(id,callback);
}
