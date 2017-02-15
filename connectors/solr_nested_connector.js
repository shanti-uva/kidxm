'use strict';

//  Design pattern assumes module is a Singleton.

const log = require('tracer').colorConsole({level: process.env.solr_log_level || 'warn'});
const debugRelatedSubjects = false;
const debugRelatedPlaces = false;

// config
var readSolrConfig = {
    'host': 'ss206212-us-east-1-aws.measuredsearch.com',
    'port': 443,
    'secure': true,
    'path': '/solr',
    'core': 'kmterms'
}

// modules
const fs = require('fs');
const http = require('http');
const async = require('async');
const chalk = require('chalk');
const _ = require('underscore');

// local
const fetchRelatedSubjects = require("../connectors/fetchRelatedSubjects").fetchRelatedSubjects;
const fetchRelatedPlaces = require("../connectors/fetchRelatedPlaces").fetchRelatedPlaces;
const sm = require('../connectors/solr_manager');

// solr client
const solr_client = sm.createSolrClient(readSolrConfig);

exports.getDocument = function (uid, documentCallback) {
    log.info("getDocument: " + uid);
    // try {
    async.waterfall(
        [
            function (cb) {
                var query = solr_client.createQuery().q("uid:" + uid);
                log.info("searching for: " + uid);
                solr_client.search(query, function (err, obj) {
                    log.info("search for: " + uid + " returned ERR: " + err + " and OBJ: " + obj);
                    if (err) {
                        log.error("Error from solr_client: %s ", err);
                        // log.info("%j",err);
                        cb(err, null);
                    } else {
                        log.info("Success from solr_client [ %s ]: %j ", uid, _.map(obj.response.docs, function (d) {
                            return d.uid + " (" + d.header + ")"
                        }));
                        // log.debug(JSON.stringify(obj, undefined, 3));
                        cb(null, obj);
                    }
                });
            },
            function (solrResp, callback) {
                try {
                    if (false) log.info(JSON.stringify(solrResp, undefined, 2));

                    // short-circuit if response is empty
                    if (!solrResp.response || !solrResp.response.docs || solrResp.response.docs.length === 0) {
                        log.info("No response! Empty Array returned.");
                        log.info("Solr response: %s", JSON.stringify(solrResp, undefined, 2));
                        callback(null, []);
                        return;
                    }

                    const kmapid = solrResp.response.docs[0].uid;

                    log.error("Calling in parallel to fetchRelatedSubjects and fetchRelatedPlaces with kmapid = " + kmapid);

                    async.parallel([
                            async.apply(fetchRelatedSubjects, kmapid),
                            async.apply(fetchRelatedPlaces, kmapid)
                        ],
                        function (err, obj) {

                            // short-circuit on error
                            if (err) {
                                log.error("ERROR RETRIEVING related subjects or places [" + kmapid + "]: " + err.stack);
                                log.error("RETURNED NODE: %j", obj);
                                callback(err);
                                return;
                            }

                            obj = _.flatten(obj);
                            var new_block = solrResp.response.docs[0];
                            new_block["block_type"] = "parent";
                            new_block["_childDocuments_"] = obj;

                            log.debug(chalk.green(JSON.stringify(new_block, undefined, 3)));
                            log.debug("======================================== Returning %j", new_block);
                            callback(null, [new_block]);
                        }
                    );
                } catch (err) {
                    log.error(err.stack);
                    log.error("%j", err);
//                        callback(err);
                }
            }
        ],
        function (err, ret) {
            log.debug("Final getDocument callback:  %s ERR: %j, RET: %j", uid, err, ret);
            if (err) {
                log.error("[ %s ] Error in final getDocument callback: %s", uid, JSON.stringify(err, undefined,2));
                documentCallback(err);
            } else {
                const childDocuments = ret[0]._childDocuments_;
                log.error("[ %s ] returning document (%s) with %d children", uid, ret[0].header, childDocuments.length);
                for (var i=0; i < childDocuments.length; i++) {
                    var doco = childDocuments[i];
                    log.debug("%j", doco);
                    log.error("\t[ %s ] %s -- %s",doco.block_child_type,doco.id,doco.related_title_s);
                }

                documentCallback(null, ret);
            }
        }
    );
    // } catch (e) {
    //     log.error("Error in getDocument()");
    //     log.error(e.stack);
    //     documentCallback(e);
    // }
}

exports.getItemIdList = function (docidlist, callback) {
    throw new Error("NOT YET IMPLEMENTED!");
};

exports.getDocumentsByKmapId = function (kmapid, callback) {
    throw new Error("NOT YET IMPLEMENTED!");
};

exports.getDocumentsByKMapIdStale = function (kmapid, staletime, callback) {
    throw new Error("NOT YET IMPLEMENTED!");
};
