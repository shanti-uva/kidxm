/**
 * Created by ys2n on 7/6/14.
 * TODO:  Needs to handle places!
 * TODO: Need to agree upon places vs subjects handling (separate namespaces or unique id's in single namespace?)
 */

var async = require('async');
var populator = require('../tasks/populator');
var _ = require('underscore');
var traverse = require('traverse');
var NodeCache = require('node-cache');
var cache = new NodeCache();


var Settings = {
    mediabase_host: 'mediabase.drupal-dev.shanti.virginia.edu',
    mediabase_port: 80,
    mediabase_kmaps_path: '/services/kmaps',
    mediabase_solr_path: '/services/solrdoc',
    kmaps_prefix: '',
    kmaps_domain: 'kmaps.virginia.edu',
    kmaps_port: 80,
    kmaps_fancy_path: '/features/fancy_nested.json'
}


exports.getItemIdList = function (kid, callback) {
//    var result  = $.ajax("http://mediabase.drupal-test.shanti.virginia.edu/services/kmaps/" + kmapid);
    var http = require('http');

    if (!kid || _.isUndefined(kid)) {
        throw new Error("You need to specify a kmap id");
    }
    // sloppy way of translating id...

    // console.log("kid = " + kid);


    var kmapid = (kid.replace) ? kid.replace('subjects-', '').replace('places-', 'p') : kid;

    var options = {
        host: Settings.mediabase_host,
        port: Settings.mediabase_port,
        path: Settings.mediabase_kmaps_path + "/" + kmapid + ".json",
        method: 'GET'
    };

    var ret = [];

    http.request(options, function (res) {
        var raw = [];
        console.log('OPTIONS: ' + JSON.stringify(options));
        console.log('STATUS: ' + res.statusCode);
        console.log('HEADERS: ' + JSON.stringify(res.headers));
        res.setEncoding('utf8');
        res.on('data', function (chunk) {
            raw.push(chunk);
        });

        res.on('end', function () {
            var obj = JSON.parse(raw.join(''));
            console.log(JSON.stringify(obj, undefined, 2));
            ret = _.unique(obj.items);
            console.log("Lookup for kmapid " + kmapid + " returns " + ret);
            callback(null, ret);
        })

    }).end();

};


//exports.getDocumentXML = function (docid, callback) {
//
//    var dom = require('xmldom').DOMParser
//    var xpath = require('xpath');
//    var parseString = require('xml2js').parseString;
//    var http = require('http');
//
//    var options = {
//        host: 'mediabase.drupal-test.shanti.virginia.edu',
//        port: 80,
//        path: '/services/solrdoc/' + docid,
//        method: 'GET'
//    };
//
//    var doc = {};
//
//    console.log("Attempting to contact: " + JSON.stringify(options));
//
//    http.request(options,function (res) {
//        var raw = [];
////        console.log('STATUS: ' + res.statusCode);
////        console.log('HEADERS: ' + JSON.stringify(res.headers));
//        res.setEncoding('utf8');
//        res.on('data', function (chunk) {
//            raw.push(chunk);
//        });
//
//        res.on('end', function () {
//            var xmldoc = new dom().parseFromString(raw.join(''));
//            console.log(xmldoc.toString());
//            var kmapids = [];
//            var kmapnodes = xpath.select("//field[@name='kmapid']/text()", xmldoc);
//            kmapnodes.forEach( function(x) {kmapids.push(
//                "subjects-" + x.nodeValue)});
//            var pdidnodes = xpath.select("//field[@name='pdid']/text()", xmldoc)
//            pdidnodes.forEach( function(x) {kmapids.push(
//                "places-" + x.nodeValue)});
//
//            doc.kmapid = _.unique(kmapids);
//            doc.url = xpath.select("//field[@name='url']/text()",xmldoc)[0].nodeValue;
//            doc.bundle = xpath.select("//field[@name='bundle']/text()",xmldoc)[0].nodeValue;
//            doc.description = xpath.select("//field[@name='content']/text()",xmldoc)[0].nodeValue;
//            doc.id = xpath.select("//field[@name='entity_id']/text()",xmldoc)[0].nodeValue;
//            doc.service = "mediabase";
//            doc.uid = doc.service + "-" + doc.id;
//            callback(null,doc);
//        })
//    }).end();
//
//
//}

exports.getDocument = function (docid, callback) {

    console.log("getDocument()");
    //console.log(new Error("oof").stack);

    var http = require('http');
    var getParentMap = function (type, callback) {

        console.log("getDocument() getParentMap() called with type=" + type);

        // consult cache

        var cached_map = cache.get(type)[type];
        var httpParams = {
            host: Settings.kmaps_prefix + type + '.' + Settings.kmaps_domain,
            port: Settings.kmaps_port,
            path: Settings.kmaps_fancy_path,
            method: 'GET'
        }
        console.log(" Getting parentMap through:  " + JSON.stringify(httpParams));

        if (_.isEmpty(cached_map)) {
            http.request(httpParams,
                function (res) {
                    var raw = [];
                    console.log("STATUS CODE: " + res.statusCode);

                    if (res.statusCode !== 200) {
                        throw new Error("ERROR CODE " + res.statusCode + " while contacting kmaps server");
                    }

                    res.setEncoding('utf8');
                    res.on('data', function (chunk) {
                        //console.log("DATA: " + chunk);
                        raw.push(chunk);
                    });
                    res.on('end', function () {
                        var map = {};
                        var tree = JSON.parse(raw.join(''));
                        console.log("RAW: " + raw);

                        traverse(tree).forEach(function () {
                            // console.log("HUHUHUHUHUH: " + this.key);
                            if (this.node && this.key === "key") {

                                //console.dir("PARENTS OF: " + this.node);
                                //console.dir(this);
                                var path = [];
                                this.parents.map(function (x, y) {
                                    if (x) {
                                        // console.dir(x.parents);
                                        var rents = x.parents.filter(function (x) {
                                            return (x.node.key ? true : false);
                                        });

                                        path = rents.map(function (x) {
                                            return type + "-" + x.node.key;
                                        })
                                    }
                                });

                                map[type + "-" + this.node] = path;

                                // console.log (type + "-" + this.node + " : "  + JSON.stringify(path));

                                // console.dir(path);
                            }
                        });
                        console.log("DONE MAPPING:");
                        //console.dir (map);

                        // cache the map
                        if (cache.set(type, map)) {
                            console.log("parentMap successfully cached for " + type + " ! size: " + _.size(map) + " ?=== " + _.size(cache.get(type)));
                        } else {
                            throw new Error("Couldn't store parentMap!");
                        }
                        callback(null, map);
                    });
                    res.on('error', function (err) {
                        console.dir(err);
                    });
                }).on('error', function (err) {
                console.error(err);
            }).end();

        } else {
            console.log("Using cached parent map for " + type);
            // console.dir(cached_map);
            callback(null, cached_map);
        }
    }

    var getSolrDoc = function (parentMap, callback) {

        var options = {
            host: Settings.mediabase_host,
            port: Settings.mediabase_port,
            path: Settings.mediabase_solr_path + "/" + docid + ".json",
            method: 'GET'
        };

        //console.dir(parentMap);

        if (_.isEmpty(parentMap)) {
            console.error("EMPTY parentMap!");
        }

        console.log("parentMap has size of: " + _.size(parentMap));

        var doc = {};

        console.log("Attempting to contact: " + JSON.stringify(options));

        http.request(options, function (res) {
            var raw = [];
            console.log('STATUS: ' + res.statusCode);
            console.log('HEADERS: ' + JSON.stringify(res.headers));
            res.setEncoding('utf8');
            res.on('data', function (chunk) {
                //console.log("DATA: " + chunk);
                raw.push(chunk);
            });

            res.on('end', function () {
                var doc = {};
                var obj;
                try {
                    switch (res.statusCode) {
                        case 200:
                            obj = JSON.parse(raw.join(''));
                            // console.log(JSON.stringify(obj,undefined,2));
                            doc = obj.doc;

                            console.log("parentMap now has size of: " + _.size(parentMap));

                            if (!_.isEmpty(parentMap)) {
                                // console.dir (parentMap);
                                doc.kmapid = doc.kmapid.map(function (x) {
                                    if (parentMap[x]) {
                                        return parentMap[x];
                                    } else {
                                        console.warn(" parentMap does not contain: " + x);
                                        return [x];  // just return itself.
                                    }
                                });
                                doc.kmapid = _.flatten(doc.kmapid);
                            }
                            doc.kmapid = _.unique(doc.kmapid);
                            console.log("Return from mediabase: ");
                            console.dir(doc);
                            callback(null, doc);
                            break;
                        case 403:
                            doc.kmapid = [];
                            doc.error = "Forbidden " + res.statusCode + " (" + http.STATUS_CODES[res.statusCode] + ")";
                            doc.status = res.statusCode;
                            callback(doc, doc);
                            break;
                        case 404:
                            doc = JSON.parse(raw.join(''));
                            doc.action = "delete";
                            doc.kmapid = [];
                            doc.status = res.statusCode;
                        default:
                            if (doc.error) {
                                doc.message = "Object deleted or missing:  return code " + res.statusCode + " (" + http.STATUS_CODES[res.statusCode] + ")";
                            } else {
                                doc.message = "Unhandled error return code " + res.statusCode + " (" + http.STATUS_CODES[res.statusCode] + ")";
                            }
                            callback(doc, doc);
                    }

                }
                catch (err) {
                    console.dir(err);
                    console.log(err.stack);
                    callback(err, doc);
                }
            })
        }).end();
    };


    async.waterfall(
        [
            function (callback) {
                getParentMap("subjects", function (err, bigMap) {
                    getParentMap("places", function (err, placesMap) {
                        _.extend(bigMap, placesMap);
                        callback(err, bigMap);
                    })
                });

            },
            getSolrDoc],
        function completed(err, ret) {
            console.log("DONE");
            callback(err, ret);
        }
    )
}

exports.getDocumentsByDocIds = function (docids, callback) {
    console.log("getDocumentsByDocIds()");

    async.waterfall((
        [
            function (cb) {
                async.mapLimit(docids, 10, exports.getDocument,
                    function (err, doc) {
                        console.log("DONE");
                        if (err) { console.log("ERROR: " + err); }
                        cb(err, doc);
                    }
                );
            }
        ],
            function (err, results) {
                if (err) {
                    console.log("ERROR: " + err);
                }
                console.log("FINALLY:  " + JSON.stringify(results));

                callback(null, results);
            }
    ));
}

exports.getDocumentsByKmapIdStale = function (kmapid, staletime, callback) {

    console.log("getDocumentsByKmapIdStale()");


    async.waterfall(
        [
            function (cb) {
                exports.getItemIdList(kmapid, cb);
            },
            function (itemIdList, callback) {

                // cull the itemIdList per timestamp
                // Hmmm.  here's where we need to cull the itemidlist
                // by check each item against the solr timestamps....
                // Messy but you should be able to do it!

                async.waterfall([
                        function (callback) {
                            // filter the list
                            async.filter(itemIdList,
                                function (item, cb2) {
                                    item = "mediabase-" + item;
                                    populator.documentStale(item, staletime, function (err, answer) {
                                        cb2(!answer);
                                    });
                                }, function (result) {
                                    callback(null, result);
                                }
                            );
                        }
                    ],
                    function (err, finalItemIdList) {
                        console.dir(finalItemIdList);
                        async.mapLimit(finalItemIdList, 2, exports.getDocument,
                            function (err, doc) {
                                // console.log(JSON.stringify(doc,undefined,3));
                                callback(null, doc);
                            }
                        )
                    })
            }
        ],
        function (err, results) {
            console.log("FINALLY:  " + JSON.stringify(results));
            callback(null, results);
        }
    );

}


exports.getDocumentsByKmapId = function (kmapid, callback) {

    console.log("getDocumentsByKmapId()");

    async.waterfall(
        [
            function (cb) {
//                console.trace("calling getItemIdList with " + kmapid);
                exports.getItemIdList(kmapid, cb);
            },
            function (itemIdList, cb) {
                async.mapLimit(itemIdList, 5, exports.getDocument,
                    function (err, doc) {
                        console.log("DONE");
                        // console.log(JSON.stringify(doc,undefined,3));
                        cb(null, doc);
                    }
                );
            }
        ],
        function (err, results) {
            // console.log("FINALLY:  " + JSON.stringify(results));
            callback(null, results);
        }
    );
}
