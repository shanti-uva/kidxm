/**
 * Created by ys2n on 12/7/16.
 */
const log = require('tracer').colorConsole({level:'warn'});
var http = require('http');
var flatten = require('../connectors/flattenRelatedSubjects').flattenRelatedSubjects;

exports.fetchRelatedSubjects = function (kmapid,callback) {

    log.debug("fetchRelatedSubjects kmapid: " + kmapid );

    var s = kmapid.split('-');

    var type = s[0];
    var kid = s[1];

    log.debug("fetchRelatedSubjects");
    log.debug("kmapid = " + kmapid);
    log.debug("fetchRelatedSubjects type = " + type);
    log.debug("kid = " + kid);

    // short circuit
    if (type === "subjects") {
        callback(null,[]);
        return;
    }

    var restCall = {
        host: 'places.kmaps.virginia.edu',
        port: 80,
        path: '/features/' + kid + "/topics.json"
    };

    // log.info("%j",restCall);
    try {
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
                    log.debug(ret);
                    var list = JSON.parse(ret);
                    var result = flatten(kmapid, list);
                    callback(null, result);
                }
                catch (err) {
                    console.log(err);
                    console.log(ret);
                    log.info("%j",restCall);
                    callback(err);
                }
                finally {
                    res.resume();
                }
            });
        }).end();
    } catch (e) {
        log.error(' %j',e);
    }
};