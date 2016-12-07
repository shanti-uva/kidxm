

// debugging flags
var debugRelatedSubjects = false;
var debugRelatedPlaces = false;

var sm = require('../connectors/solrmanager');
var http = require('http');
var async = require('async');
var _ = require('underscore');
var flattenRelationTypes = require('../connectors/flattenRelationTypes').flattenRelationTypes;
var flattenRelatedSubjects = require('../connectors/flattenRelatedSubjects').flattenRelatedSubjects;
var fetchRelatedSubjects = require("../connectors/fetchRelatedSubjects").fetchRelatedSubjects;
var fetchRelatedPlaces = require ("../connectors/fetchRelatedPlaces").fetchRelatedPlaces;
var chalk = require('chalk');

// var fetchRelatedSubjects = function (kid,callback) {
//
//     // var kid = doc.response.docs[0].uid.split('-', 2)[1];
//
//     var relatedSubjectsRest = {
//         host: 'places.kmaps.virginia.edu',
//         port: 80,
//         path: '/features/' + kid + "/topics.json",
//     };
//
//     http.request(relatedSubjectsRest, function (res) {
//         var raw = [];
//         res.setEncoding('utf8');
//         res.on('error', function (e) {
//             callback(e, null);
//         });
//         res.on('data', function (chunk) {
//             raw.push(chunk);
//         });
//         res.on('end', function () {
//             try {
//                 var related_subjects = JSON.parse(raw.join(''));
//                 var finalRelatedSubjects = flattenRelatedSubjects(related_subjects);
//                 callback(null, finalRelatedSubjects);
//             }
//             catch (err) {
//                 callback(err, null);
//             }
//             finally {
//                 res.resume();
//             }
//         });
//     }).end();
//
//
// };
//
// var fetchRelatedPlaces = function (kid,relatedPlacesCallback) {
//
//     var relatedPlacesRest = {
//         host: 'places.kmaps.virginia.edu',
//         port: 80,
//         path: '/features/' + kid + "/related.json"
//     };
//
//     http.request(relatedPlacesRest, function (res) {
//         var raw = [];
//         res.setEncoding('utf8');
//         res.on('error', function (e) {
//             relatedPlacesCallback(e, null);
//         });
//
//         res.on('data', function (chunk) {
//             raw.push(chunk);
//         });
//
//         res.on('end', function () {
//             try {
//                 var obj = JSON.parse(raw.join(''));
//                 var relation_types = obj.feature_relation_types;
//                 var finalRelatedPlaces = flattenRelationTypes(relation_types);
//                 relatedPlacesCallback(null, finalRelatedPlaces);
//             }
//             catch (err) {
//                 relatedPlacesCallback(err, null);
//             }
//             finally {
//                 res.resume();
//             }
//         });
//
//     }).end();
//
// };


// init: solrclient
var solr = require('solr-client');
var sourceConfig = {
    'host': 'localhost',
    'port': 8983,
    'path': '/solr',
    'core': 'termindex'
}
var solrclient = solr.createClient(sourceConfig);

exports.getDocument = function (uid, getDocumentCallback) {

    try {
        async.waterfall([
                function (cb) {
                    try {
                        var query = solrclient.createQuery().q("uid:" + uid);
                        solrclient.search(query, function (err, obj) {
                            if (err) {
                                console.error("Error from solrclient: " + JSON.stringify(solrclient.options));
                                console.dir(err);
                                cb(err, null);
                            } else {
                                cb(null, obj);
                            }
                        });
                    } catch (serr) {
                        console.log("error during solrclient.search");
                        console.dir(serr);
                        cb(serr);
                    } finally {
                    }
                },
                function (solrResp, callback) {
                    try {
                        if(false) console.dir(solrResp, {depth: 2, colors: true});

                        // null guard for when solr query returns nothing
                        if(!solrResp.response || !solrResp.response.docs || solrResp.response.docs.length === 0) {
                            callback([]);
                            return;
                        }

                        kid = solrResp.response.docs[0].uid.split('-', 2)[1]
                        async.parallel([
                                async.apply(fetchRelatedSubjects, kid),
                                async.apply(fetchRelatedPlaces, kid)
                            ],
                            function (err, obj) {
                                if (false) {
                                    console.log("parallel returned with:");
                                    console.dir(err);
                                    console.dir(obj);
                                }

                                if (err) {
                                    console.error("Error:" + err);
                                    callback(err);
                                }
                                else {
                                    obj = _.flatten(obj);
                                    if (false) {
                                        console.log("=====\nbobo:");
                                        console.dir(obj);
                                        console.log("=====");
                                    }
                                    callback(obj);
                                }
                            }
                        );
                    } catch (err) {
                        console.log("error");
                        console.dir(err);
                    }
                }],
            function (ret) {
                if (false) {
                    console.log("zingo");
                    console.dir(ret);
                }
                getDocumentCallback(null, ret);
            }
        );
    } catch(e) {
        console.error("Error in getDocument()");
        console.dir(e);
    }
}

exports.getItemIdList = function (docidlist, callback) {

};

exports.getDocumentsByKmapId = function (kmapid, callback) {

};

exports.getDocumentsByKMapIdStale = function (kmapid, staletime, callback) {

};
