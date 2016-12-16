const log = require('tracer').colorConsole({ level:'warn'});

// debugging flags
var debugRelatedSubjects = false;
var debugRelatedPlaces = false;
var chalk = require('chalk');

var sm = require('../connectors/solr_manager');
var http = require('http');
var async = require('async');
var _ = require('underscore');
// var flattenRelationTypes = require('../connectors/flattenRelationTypes').flattenRelationTypes;
// var flattenRelatedSubjects = require('../connectors/flattenRelatedSubjects').flattenRelatedSubjects;
var fetchRelatedSubjects = require("../connectors/fetchRelatedSubjects").fetchRelatedSubjects;
var fetchRelatedPlaces = require("../connectors/fetchRelatedPlaces").fetchRelatedPlaces;
var chalk = require('chalk');

// init: solrclient
var solr = require('solr-client');
var sourceConfig = {
    'host': 'ss558499-us-east-1-aws.measuredsearch.com',
    'port': 443,
    'secure': true,
    'path': '/solr',
    'core': 'kmterms_test'
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
                                // log.info("%j",err);
                                cb(err, null);
                            } else {
                                // // log.info("%j",obj, { depth: 5 , colors: true});
                                cb(null, obj);
                            }
                        });
                    } catch (serr) {
                        log.info("error during solrclient.search");
                        // log.info("%j",serr);
                        cb(serr);
                    } finally {
                    }
                },
                function (solrResp, callback) {
                    try {
                        // if(true) // log.info("%j",solrResp, {depth: 2, colors: true});

                        // null guard for when solr query returns nothing
                        if (!solrResp.response || !solrResp.response.docs || solrResp.response.docs.length === 0) {
                            callback([]);
                            return;
                        }

                        kmapid = solrResp.response.docs[0].uid;

                        log.debug("crooky====" + kmapid);
                        log.debug("FROOKY====" + JSON.stringify(solrResp.response.docs[0], undefined, 3));

                        async.parallel([
                                async.apply(fetchRelatedSubjects, kmapid),
                                async.apply(fetchRelatedPlaces, kmapid)
                            ],

                            function (err, obj) {
                                if (false) {
                                    // log.info("parallel returned with:");
                                    // log.info("%j",err);
                                    // log.info("%j",obj);
                                }

                                if (err) {
                                    log.error("Error:" + err);
                                    callback(err);
                                }
                                else {
                                    obj = _.flatten(obj);
                                    if (false) {
                                        log.debug("=====\nbobo:");
                                        // log.info("%j",obj);
                                        log.debug("=====");
                                    }

                                    var diddly = solrResp.response.docs[0];
                                    diddly["block_type"] = "parent";
                                    diddly["_childDocuments_"] = obj;
                                    log.debug(chalk.green(JSON.stringify(diddly, undefined, 3)));
                                    callback([diddly]);
                                }
                            }
                        );
                    } catch (err) {
                        log.debug("error");
                        // log.info("%j",err);
                    }
                }],
            function (ret) {
                getDocumentCallback(null, ret);
            }
        );
    } catch (e) {
        log.error("Error in getDocument()");
        log.info("%j",e);
    }
}

exports.getItemIdList = function (docidlist, callback) {

};

exports.getDocumentsByKmapId = function (kmapid, callback) {

};

exports.getDocumentsByKMapIdStale = function (kmapid, staletime, callback) {

};
