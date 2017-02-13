'use strict';
/**
 * Created by ys2n on 12/7/16.
 */
const log = require('tracer').colorConsole({level:process.env.solr_log_level||'warn'});
const http = require('http');
const url = require('url');
const flattenPlacesRelatedSubjects = require('../connectors/flattenRelatedSubjects').flattenRelatedSubjects;
const flattenSubjectsRelatedSubjects = require('../connectors/flattenRelationTypes').flattenRelationTypes;

const async = require('async');

exports.fetchRelatedSubjects = function (kmapid,callback) {

    log.debug("fetchRelatedSubjects kmapid: " + kmapid );

    var s = kmapid.split('-');
    var type = s[0];
    var kid = s[1];

    log.debug("fetchRelatedSubjects");
    log.debug("kmapid = " + kmapid);
    log.debug("fetchRelatedSubjects type = " + type);
    log.debug("kid = " + kid);

    var placesRestCall = {
        host: 'places.kmaps.virginia.edu',
        port: 80,
        path: '/features/' + kid + "/topics.json"
    };

    var subjectsRestCall = {
        host: 'subjects.kmaps.virginia.edu',
        port: 80,
        path: '/features/' + kid + "/related.json"
    }

    var restCall = undefined;
    var flatten = undefined;
    var whichRest = "", whichFlat = "";

    if (type === "subjects") {
        restCall = subjectsRestCall;
        flatten = flattenSubjectsRelatedSubjects;
        whichRest = "subjectsRestCall";
        whichFlat = "flattenSubjectsRelatedSubjects";
    } else {
        restCall = placesRestCall;
        flatten = flattenPlacesRelatedSubjects;
        whichRest = "placesRestCall";
        whichFlat = "flattenPlacesRelatedSubjects";
    }

    log.info("====> [ %s ] %s %s", kmapid, whichRest, whichFlat );

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
                log.debug("%s",ret);
                var list = JSON.parse(ret);
                var result = flatten(kmapid,list);
                log.debug("calling back with %j", result);
                callback(null, result);
            }
            catch (err) {
                log.error(err.stack);
                log.error(ret);
                log.error("Error fetching from url %s: %s", url.format(restCall), err);
                log.error("%j", restCall);
                callback(err);
            }
            finally {
                res.resume();
            }
        });
    }).end();

};