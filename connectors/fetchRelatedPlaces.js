/**
 * Created by ys2n on 12/7/16.
 */
const debug = require('debug')('fetchRelatedPlaces');

var http = require('http');
var flatten = require('../connectors/flattenRelationTypes').flattenRelationTypes; //different

exports.fetchRelatedPlaces = function (kmapid,callback) {

    debug("fetchRelatedPlaces kmapid: " + kmapid );

    var s = kmapid.split('-');
    var type = s[0];
    var kid = s[1];

    debug("kmapid = " + kmapid);
    debug("fetchRelatedPlaces type = " + type);
    debug("kid = " + kid);

    var restCall = {
        host: type + '.kmaps.virginia.edu',
        port: 80,
        path: '/features/' + kid + "/related.json" // different
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

                // console.dir(ret);

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