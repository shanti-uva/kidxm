const log = require('tracer').colorConsole({level: process.env.solr_log_level});

// debugging flags
var debugRelatedSubjects = false;
var debugRelatedPlaces = false;
var chalk = require('chalk');
const fs = require('fs');

var sm = require('../connectors/solr_manager');
var http = require('http');
var async = require('async');
var _ = require('underscore');
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

exports.getDocument = function (uid, getDocumentCallback) {
    // console.error(new Error("poop:").stack);
    log.info("getDocument: " + uid);

    var FORCE_WRITE = process.env.solr_write_force;

    log.info("FORCE WRITE = " + FORCE_WRITE);

    if (process.env.solr_write_user) {
        log.info("process.env.solr_write_user: " + process.env.solr_write_user);
    } else {
        log.warn("process.env.solr_write_user is not set!");
    }

    if (process.env.solr_write_password) {
        log.debug("process.env.solr_write_password: ( length: %s )", process.env.solr_write_password.length);
    } else {
        log.warn("process.env.solr_write_password is not set!");
    }

    var solrclient = solr.createClient(sourceConfig).basicAuth(process.env.solr_write_user, process.env.solr_write_password);

    log.info("Checking " + uid);

    const filepath = "./output/" + uid;

    var now = new Date().getTime();
    // log.error("Date.getTime = " + now);


    log.debug(filepath + " exists? " + fs.existsSync(filepath));

    var modtime = (fs.existsSync(filepath)) ? fs.statSync(filepath).mtime : 0;   // TODO:  mtime is INCORRECT
    var staletime = now - modtime;
    const scalefactor = 1000; // (one second)
    const stalethresh = process.env.solr_write_stalethresh || 100 * scalefactor;

    log.debug("now       = " + now / scalefactor);
    log.debug("modtime   = " + modtime);
    log.debug("staletime = " + staletime / scalefactor);
    log.debug("stalethresh = " + stalethresh / scalefactor);

    if (FORCE_WRITE || (staletime > stalethresh)) {
        log.info("stale or missing entry found: " + uid + "( staletime: " + staletime + " stalethresh: " + stalethresh + " FORCE_WRITE:  " + FORCE_WRITE);
        try {
            async.waterfall(
                [
                    function (cb) {
                        try {
                            var query = solrclient.createQuery().q("uid:" + uid);
                            solrclient.search(query, function (err, obj) {
                                if (err) {
                                    log.error("Error from solrclient: " + JSON.stringify(err));
                                    // log.info("%j",err);
                                    cb(err, null);
                                } else {
                                    log.info("Success from solrclient [ %s ]: %j ", uid, _.map(obj.response.docs, function (d) {
                                        return d.uid + " (" + d.header + ")"
                                    }));
                                    // log.debug(JSON.stringify(obj, undefined, 3));
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
                            if(true) log.info(JSON.stringify(solrResp,undefined,2));


                            // null guard for when solr query returns nothing
                            if (!solrResp.response || !solrResp.response.docs || solrResp.response.docs.length === 0) {
                                log.info("No response! NULL RETURNED");
                                log.info(JSON.stringify(solrResp, undefined, 2));
                                callback(null, []);
                                return;
                            }

                            kmapid = solrResp.response.docs[0].uid;

                            log.info("Calling in parallel to fetchRelatedSubjects and fetchRelatedPlaces with kmapid = " + kmapid);


                            // if (false && staletime > 600000000) {
                            async.parallel([
                                    async.apply(fetchRelatedSubjects, kmapid),
                                    async.apply(fetchRelatedPlaces, kmapid)
                                ],
                                function (err, obj) {
                                    if (err) {
                                        // short circuit
                                        log.error("ERROR RETRIEVING related subjects or places [" + kmapid + "]: " + err.stack);
                                        log.error("RETURNED NODE: %j", obj);
                                        callback(err);
                                        return;
                                    }

                                    obj = _.flatten(obj);
                                    var diddly = solrResp.response.docs[0];
                                    diddly["block_type"] = "parent";
                                    diddly["_childDocuments_"] = obj;
                                    log.debug(chalk.green(JSON.stringify(diddly, undefined, 3)));

                                    const jsonString = JSON.stringify(diddly, undefined, 3);
                                    log.debug("GUMPSCUMP=== %s ===", diddly.uid);

                                    fs.writeFile(filepath, jsonString, function (err) {
                                        if (err) {
                                            log.error("ERROR writing " + filepath + ": " + err);
                                        } else {
                                            log.error("The file \"%s\" was saved.", filepath);
                                        }
                                    })
                                    log.info("======================================== Returning %j", diddly);
                                    callback(null, [diddly]);

                                }
                            );
                        } catch (err) {
                            log.info(err.stack);
                            log.info("%j", err);
                            callback(err);
                        }
                    }
                ],
                function (err, ret) {
                    log.info("Final getDocument callback:  %s ERR: %j, RET: %j",uid, err, ret);
                    getDocumentCallback(err, ret);
                }
            );
        } catch (e) {
            log.error("Error in getDocument()");
            log.error(e.stack);
        }
    } else {
        log.info ("getDocument callback:  " + uid + " <null>");
        async.nextTick(function () {
            getDocumentCallback(null, []);
        });
    }
}

exports.getItemIdList = function (docidlist, callback) {

};

exports.getDocumentsByKmapId = function (kmapid, callback) {

};

exports.getDocumentsByKMapIdStale = function (kmapid, staletime, callback) {

};
