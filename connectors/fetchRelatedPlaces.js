/**
 * Created by ys2n on 12/7/16.
 */
const log = require('tracer').colorConsole({level:'warn'});
var http = require('http');
var flatten = require('../connectors/flattenRelationTypes').flattenRelationTypes; //different

exports.fetchRelatedPlaces = function (kmapid,callback) {

    log.info("fetchRelatedPlaces kmapid: " + kmapid );

    var s = kmapid.split('-');
    var type = s[0];
    var kid = s[1];

    log.debug("kmapid = " + kmapid);
    log.debug("fetchRelatedPlaces type = " + type);
    log.debug("kid = " + kid);

    var restCall = {
        host: type + '.kmaps.virginia.edu',
        port: 80,
        path: '/features/' + kid + "/related.json" // different
    };
    log.debug('%j',restCall);
try {
    http.request(restCall, function (res) {
        var raw = [];
        log.debug(JSON.stringify(res.headers,undefined,3));
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

                log.debug("========" + JSON.stringify(list, undefined,3));



                var result = flatten(kmapid,list);

                log.debug ("Returning %j", result );
                log.debug ("result length: " + result.length);


                callback(null, result);
            }
            catch (err) {
                log.error(err);
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