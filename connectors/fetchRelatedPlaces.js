/**
 * Created by ys2n on 12/7/16.
 */
'use strict';
const TIMEOUT = 1200 * 1000; // TIMEOUT in millis
const log = require('tracer').colorConsole({level: 'warn'});
const http = require('http');
const flattenRelationTypes = require('../connectors/flattenRelationTypes').flattenRelationTypes; //different
const flattenRelatedPlaces = require("../connectors/flattenRelatedPlaces").flattenRelatedPlaces;
const async = require('async');
const _ = require("underscore");
const JSON = require("circular-json");
const url = require('url');

exports.fetchRelatedPlaces = function (kmapid, callback) {

    log.info("fetchRelatedPlaces kmapid: " + kmapid);

    var s = kmapid.split('-');
    var type = s[0];
    var kid = s[1];

    log.debug("fetchRelatedPlaces");
    log.debug("kmapid = " + kmapid);
    log.debug("fetchRelatedPlaces type = " + type);
    log.debug("kid = " + kid);

    var restCallPlaces = {
        protocol: "http:",
        hostname: 'places.kmaps.virginia.edu',
        pathname: '/features/' + kid + "/related.json"
    };

    var restCallSubjects = {
        protocol: "http:",
        hostname: 'places.kmaps.virginia.edu',
        pathname: '/topics/' + kid + ".json"
    };

    var restCall = undefined;
    var flatten = undefined;

    log.info(" kmapid = " + kmapid);
    log.info(" type = " + type);

    //if (type === "places"){
    restCall = restCallPlaces;
    flatten = flattenRelationTypes;
    // } else {
    //     restCall = restCallSubjects;
    //     flatten = flattenRelatedPlaces;
    // }

    log.debug('BASE URL: %s (%j)', url.format(restCall), restCall);


    var more = true;
    var remaining_pages = "<unknown>";
    var page = 1;
    // async.doUntil(
    //     function(cb) {
    // restCall.search ='?per_page=50&page=' + page;
    log.debug("Rest Call: [ %s ]: %j", kmapid, restCall);
    log.debug("Rest Url: %s", url.format(restCall));
    var req = http.request(url.format(restCall), function (res) {

        log.info(JSON.stringify(res, undefined, 3));

        var raw = [];
        res.setEncoding('utf8');
        res.on('error', function (e) {
            log.error(e);
            log.error("Rest Url: %s", url.format(restCall));
        });

        res.on('data', function (chunk) {
            raw.push(chunk);
        });

        res.on('end', function () {
            try {
                var ret = raw.join('');
                var list = JSON.parse(ret);
                log.debug(JSON.stringify(list, undefined, 3));
                var result = flatten(kmapid, list);
                log.debug("Returning: " + result);
                remaining_pages = (result.length) ? result[0].remaining_pages : 0;
                more = (remaining_pages !== 0);
                page++;
                callback(null, result);
            }
            catch (err) {
                log.info(err);
                log.error(ret);
                log.error("Error fetching from url %s: %s", url.format(restCall), err);
                callback(new Error("error parsing related places: %s " + err.message, err));
            }
            finally {
                res.resume();
            }
        });
    });
    req.on('socket', function (socket) {
        socket.setTimeout(TIMEOUT);
        socket.on('timeout', function () {
            req.abort();
        });
    });

    req.on('error', function(err) {
        console.error("Connection lost!");
        console.error(err);
        req.abort();
        callback(err);
    });


    req.end();
};