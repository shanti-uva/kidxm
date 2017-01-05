/**
 * Created by ys2n on 12/7/16.
 */
const log = require('tracer').colorConsole({level:'warn'});
var http = require('http');
var flatten = require('../connectors/flattenRelationTypes').flattenRelationTypes; //different
var async = require('async');

exports.fetchRelatedPlaces = function (kmapid,callback) {

    log.info("fetchRelatedPlaces kmapid: " + kmapid );

    var s = kmapid.split('-');
    var type = s[0];
    var kid = s[1];

    log.debug("fetchRelatedPlaces");
    log.debug("kmapid = " + kmapid);
    log.debug("fetchRelatedPlaces type = " + type);
    log.debug("kid = " + kid);

    var restCall = {
        host: type + '.kmaps.virginia.edu',
        port: 80,
        path: '/features/' + kid + "/related.json" // different
    };
    log.debug('%j',restCall);

    async.retry (
        100,
        function() {
            log.info ("Rest Call: [ %s ]: %j", kmapid, restCall);
            http.request(restCall, function (res) {
                var raw = [];
                res.setEncoding('utf8');
                res.on('error', function (e) {
                    callback(e, null);
                });

                res.on('data', function (chunk) {
                    raw.push(chunk);
                });

                res.on('end', function () {
                    try {
                        var ret = raw.join('');
                        var list = JSON.parse(ret);
                        var result = flatten(kmapid,list);
                        log.info("Returning: " + result);
                        callback(null, result);
                    }
                    catch (err) {
                        log.info(err);
                        log.info(ret);
                        log.info("%j", restCall);
                        callback(new Error("error parsing related places: " + err.message,err));
                    }
                    finally {
                        res.resume();
                    }
                });
            }).end();
        },
        function(err,result) {
            log.error(" err = " + err + ", result = " + result);
        });
};