/**
 * Created by ys2n on 12/7/16.
 */
const debug = require('debug')('fetchRelatedSubjects');
var http = require('http');
var flatten = require('../connectors/flattenRelatedSubjects').flattenRelatedSubjects;

exports.fetchRelatedSubjects = function (kmapid,callback) {

    debug("fetchRelatedSubjects kmapid: " + kmapid );

    var s = kmapid.split('-');

    var type = s[0];
    var kid = s[1];

    debug("fetchRelatedSubjects");
    debug("kmapid = " + kmapid);
    debug("fetchRelatedSubjects type = " + type);
    debug("kid = " + kid);

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

    // console.dir(restCall);

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
                debug(ret);
                var list = JSON.parse(ret);
                var result = flatten(kmapid,list);
                callback(null, result);
            }
            catch (err) {
                console.log(err);
            }
            finally {
                res.resume();
            }
        });
    }).end();
};