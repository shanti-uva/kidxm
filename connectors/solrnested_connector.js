const debug = require('debug')('solrnested_connector');

// debugging flags
var debugRelatedSubjects = false;
var debugRelatedPlaces = false;
var chalk = require('chalk');

var sm = require('../connectors/solrmanager');
var http = require('http');
var async = require('async');
var _ = require('underscore');
// var flattenRelationTypes = require('../connectors/flattenRelationTypes').flattenRelationTypes;
// var flattenRelatedSubjects = require('../connectors/flattenRelatedSubjects').flattenRelatedSubjects;
var fetchRelatedSubjects = require("../connectors/fetchRelatedSubjects").fetchRelatedSubjects;
var fetchRelatedPlaces = require ("../connectors/fetchRelatedPlaces").fetchRelatedPlaces;
var chalk = require('chalk');

// init: solrclient
var solr = require('solr-client');
var sourceConfig = {
    'host': 'localhost',
    'port': 8983,
    'path': '/solr',
    'core': 'termindex'
}
var solrclient = solr.createClient(sourceConfig);

exports.getDocument = function (uid, getDocumentCallback) {

    try {
        async.waterfall([
                function (cb) {
                    try {
                        var query = solrclient.createQuery().q("uid:" + uid);
                        solrclient.search(query, function (err, obj) {
                            if (err) {
                                console.error("Error from solrclient: " + JSON.stringify(solrclient.options));
                                // console.dir(err);
                                cb(err, null);
                            } else {
                                // // console.dir(obj, { depth: 5 , colors: true});
                                cb(null, obj);
                            }
                        });
                    } catch (serr) {
                        console.log("error during solrclient.search");
                        // console.dir(serr);
                        cb(serr);
                    } finally {
                    }
                },
                function (solrResp, callback) {
                    try {
                        if(true) // console.dir(solrResp, {depth: 2, colors: true});

                        // null guard for when solr query returns nothing
                        if(!solrResp.response || !solrResp.response.docs || solrResp.response.docs.length === 0) {
                            callback([]);
                            return;
                        }

                        kmapid = solrResp.response.docs[0].uid;

                        debug("crooky====" + kmapid);
                        debug("FROOKY====" + JSON.stringify(solrResp.response.docs[0],undefined,3));

                        async.parallel([
                                async.apply(fetchRelatedSubjects, kmapid),
                                async.apply(fetchRelatedPlaces, kmapid)
                            ],

                            function (err, obj) {
                                if (false) {
                                    // console.log("parallel returned with:");
                                    // console.dir(err);
                                    // console.dir(obj);
                                }

                                if (err) {
                                    console.error("Error:" + err);
                                    callback(err);
                                }
                                else {
                                    obj = _.flatten(obj);
                                    if (false) {
                                        debug("=====\nbobo:");
                                        // console.dir(obj);
                                        debug("=====");
                                    }

                                    var diddly = solrResp.response.docs[0];
                                    diddly["nest_type"] = "parent";
                                    diddly["_childDocuments_"] = obj;
                                    debug(chalk.green(JSON.stringify(diddly,undefined,3)));
                                    callback([ diddly ]);
                                }
                            }
                        );
                    } catch (err) {
                        debug("error");
                        // console.dir(err);
                    }
                }],
            function (ret) {
                getDocumentCallback(null, ret);
            }
        );
    } catch(e) {
        console.error("Error in getDocument()");
        // console.dir(e);
    }
}

exports.getItemIdList = function (docidlist, callback) {

};

exports.getDocumentsByKmapId = function (kmapid, callback) {

};

exports.getDocumentsByKMapIdStale = function (kmapid, staletime, callback) {

};
