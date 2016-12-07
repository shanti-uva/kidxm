/**
 * Created by ys2n on 12/7/16.
 */
var http = require('http');
var flatten = require('../connectors/flattenRelationTypes').flattenRelationTypes;

exports.fetchRelatedPlaces = function (kid,callback) {

    var restCall = {
        host: 'places.kmaps.virginia.edu',
        port: 80,
        path: '/features/' + kid + "/related.json" // diffe
    };

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
                var list = JSON.parse(ret).feature_relation_types;
                var result = flatten(kid,list);
                callback(null, result);
            }
            catch (err) {
                console.dir(err);
            }
            finally {
                res.resume();
            }
        });
    }).end();
};