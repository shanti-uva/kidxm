/**
 * Created by ys2n on 7/6/14.
 *
 *  TODO: refactor to handle places vs. subjects id spaces.
 *  TODO: should there be one id space and unique prefixes?  Or two different id spaces (and overlapping id's)?
 */


var async = require('async');
var populator = require('../tasks/populator');

const SERVICE = "sharedshelf";
const API_HOST = "badger.drupal-dev.shanti.virginia.edu";
const BASE_URL = "http://" + API_HOST;

const SUBJECTS_FIELD = "fd_24809_lookup";
const PLACES_FIELD = "fd_24803_lookup";
const DESCRIPTION_FIELD = "fd_248100_s";
const SUBJECTS_PREFIX = "UVA-KM-S-";
const PLACES_PREFIX = "UVA-KM-P-";

exports.getItemIdList = function (id, callback) {
    var http = require('http');

//    console.trace("getItemIdlist called with id = " + id);
    var ssid=translateKmapIdtoSharedShelfId(id);
    var field = "";

    if (ssid.lastIndexOf(SUBJECTS_PREFIX,0) === 0) {
        field = SUBJECTS_FIELD;
        console.log("Searching subjects field: " + SUBJECTS_FIELD);
    } else if (ssid.lastIndexOf(PLACES_PREFIX,0) === 0) {
        field = PLACES_FIELD;
        console.log("Searching places field: " + PLACES_FIELD);
    } else {
        field = SUBJECTS_FIELD;

        console.log ("Shared Shelf connector:  kmapid format NOT RECOGNIZED:  " + ssid + "  Searching subjects field: " + SUBJECTS_FIELD);

    }

    var options = {
        host: API_HOST,
        port: 80,
        path: '/sharedshelf/api/projects/534/assets/filter/' + field + '.links.id/' + ssid + '.json',
        method: 'GET'
    };

    var ret = [];

    http.request(options,function (res) {
        var raw = [];
//        console.log('STATUS: ' + res.statusCode);
//        console.log('HEADERS: ' + JSON.stringify(res.headers));
        res.setEncoding('utf8');
        res.on('data', function (chunk) {
            raw.push(chunk);
        });

        res.on('end', function () {
            var obj = JSON.parse(raw.join(''));
//            console.log(JSON.stringify(obj, undefined, 3));

            var idlist = [];
            for (var i=0; i < obj.assets.length; i++) {
                idlist.push(obj.assets[i].id);
            }
            callback(null, idlist);
        })

    }).end();

};

exports.getDocument = function (docid, callback) {

    var http = require('http');

    var options = {
        host: API_HOST,
        port: 80,
        path: '/sharedshelf/api/assets/' + docid + '.json',
        method: 'GET'
    };

    var doc = {};

    http.request(options,function (res) {
        var raw = [];
//        console.log('OPTIONS:' + JSON.stringify(options, undefined, 3));
//        console.log('STATUS: ' + res.statusCode);
//        console.log('HEADERS: ' + JSON.stringify(res.headers, undefined, 3));
        res.setEncoding('utf8');
        res.on('data', function (chunk) {
            raw.push(chunk);
        });

        res.on('end', function () {
            var idlist =[];
            var read = JSON.parse(raw.join(''));
            var doc = {};
            if (read.asset) {
                var obj = read.asset[0];

//                console.log("GOOPER: " + JSON.stringify(obj,undefined,3));

                // array of places
                if (obj[PLACES_FIELD]) {
                    var plist = obj[PLACES_FIELD].links;
                    console.log("FOUND " + plist.length + " places kmapids");

                    for (var i=0; i < plist.length; i++) {
                        idlist.push(translateSharedShelfIdtoKmapId(plist[i].id));
                    }
                }

                // array of subjects
                if (obj[SUBJECTS_FIELD]) {
                    var slist = obj[SUBJECTS_FIELD].links;

                    console.log("FOUND " + slist.length + " subject kmapids");

                    for (var j=0; j < slist.length; j++) {
                        idlist.push(translateSharedShelfIdtoKmapId(slist[j].id));
                    }
                }
                doc.service = SERVICE;
                doc.id = obj.id;
                doc.uid = doc.service + "-" + obj.id;
                doc.kmapid = idlist;
                doc.url = BASE_URL + "/sharedshelf" + "/THIS_ISN'T_YET_KNOWN!";
                doc.bundle = "image";
                doc.summary = obj[ DESCRIPTION_FIELD];
            }
            callback(null,doc);

        })
    }).end();

}

exports.getDocumentsByKmapId = function(kmapid, callback) {

    async.waterfall(
        [
            function(cb) {
                exports.getItemIdList(kmapid,cb);
            },
            function(itemIdList, cb) {
                async.mapLimit(itemIdList, 2, exports.getDocument,
                    function(err,doc) {
                        // console.log(JSON.stringify(doc,undefined,3));
                        cb(null,doc);
                    }
                );
            }
        ],
        function(err,results) {
            // console.log("FINALLY:  " + JSON.stringify(results));
            callback(null,results);
        }
    );

}

exports.getDocumentsByKmapIdStale = function(kmapid, staletime, callback) {
    async.waterfall(
        [
            function(idlist) {
                exports.getItemIdList(kmapid,idlist);
            },
            function(itemIdList, callback) {

                // cull the itemIdList per timestamp


                // Hmmm.  here's where we need to cull the itemidlist
                // by check each item against the solr timestamps....
                // Messy but you should be able to do it!

                async.waterfall([
                    function(callback) {
                        // filter the list
                        async.filter(itemIdList,
                            function (item, cb2) {
                                item = "sharedshelf-" + item;
                                populator.documentStale(item, staletime, function (err, answer) {
                                    cb2(!answer);
                                } );
                            },function(result) {
                                callback(null,result);
                            }
                        );
                    }
                ],
                function(err,finalItemIdList) {
                    console.dir(finalItemIdList);
                    async.mapLimit(finalItemIdList, 2, exports.getDocument,
                        function(err,doc) {
                            // console.log(JSON.stringify(doc,undefined,3));
                            callback(null,doc);
                        }
                    )
                })
            }
        ],
        function(err,results) {
            callback(null,results);
        }
    );

}


function translateKmapIdtoSharedShelfId(kmapid) {
        return String(kmapid).replace('places-','UVA-KM-P-').replace('subjects-','UVA-KM-S-');
}

function translateSharedShelfIdtoKmapId(ssid) {
        return String(ssid).replace('UVA-KM-S-', 'subjects-').replace('UVA-KM-P-','places-');
}
