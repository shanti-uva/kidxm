/**
 * Created by ys2n on 8/7/14.
 */

const VERSION = 11;
var _ = require('underscore');
var traverse = require('traverse');
var http = require('http');
var crypto = require('crypto');
var util = require('util');
var domain = require('domain');
// var async = require('async');


var napi_count = 0;
var kget_count = 0;
var kmap_count = 0;

var Settings = {
    kmaps_prefix: "dev-",
    kmaps_domain: "kmaps.virginia.edu",
    kmaps_port: 80,
    // kmaps_fancy_path: '/features/fancy_nested.json'
    kmaps_fancy_path: '/features/list.json'
};



function grokKClass(kmapid) {
    var parts = kmapid.split('-');
    var kclass = parts[0];
    var kid = parts[1];

    // sanity check kclass
    if (kclass !== "subjects" && kclass !== "places") {
        throw new Error("unknown kclass " + kclass + " in kmapid " + kmapid);
    }
    return {kclass: kclass, kid: kid};
}
exports.getKmapsDocument = function (kmapid, callback) {

    // This is a "universal" kmapid.   Translate!
    var __ret = grokKClass(kmapid);
    var kclass = __ret.kclass;
    var kid = __ret.kid;
    var pre = Settings.kmaps_prefix;


    var options = {
        host: pre + kclass + '.' + Settings.kmaps_domain,
        port: 80,
        path: '/features/' + kid + ".json",
        method: 'GET'
    };

    var doc = {};

    // console.log("Attempting to contact: " + JSON.stringify(options));

    var dom = domain.create();

    dom.on('error', function(er) {
        console.trace("UnCaught exception!");
        console.error(er.stack);
        callback(er);
    });

    dom.run( function () {
        try {
            http.request(options, function (res) {
                var raw = [];

                var doc = {};
		
		if (++kget_count > 1) console.log("COUNT kget= " + ( kget_count ));

                //console.log('STATUS: ' + res.statusCode);
                //console.log('HEADERS: ' + JSON.stringify(res.headers));
                res.setEncoding('utf8');
                res.on('error', function (e) {
                    // General error, i.e.
                    //  - ECONNRESET - server closed the socket unexpectedly
                    //  - ECONNREFUSED - server did not listen
                    //  - HPE_INVALID_VERSION
                    //  - HPE_INVALID_STATUS
                    //  - ... (other HPE_* codes) - server returned garbage

                    console.log("WHACKA")

                    console.log(e);
                    callback(null, null);
                });
                res.on('data', function (chunk) {
                    raw.push(chunk);
                });


                res.on('end', function () {
                    var abbreviate = function (str) {
                        var code = null;
                        if (typeof str == 'string') {
                            code = str;
                        } else {
                            code = (str.code.length != 0)?str.code:str.name;
                        }
                        if (code.match(/[A-Z\s]/)) {
                            var parts = code.match(/(\b[A-z]{1,3})/g);
                            code = parts.join("_").toLowerCase();
                        }
                        return code

                    }
                    try {
                        var obj = JSON.parse(raw.join(''));
                        doc._version_i = VERSION;

 //                       console.log("HOOOOOOOOOOOOOOPIE:" + JSON.stringify(obj, undefined, 2));

                        if (res.headers.etag) {
                            doc.etag = res.headers.etag
                        }
                        // ID should be unique
                        doc.id = kclass + "-" + obj.feature.id;

                        // Header
                        doc.header = obj.feature.header;

                        doc.tree = kclass;

                        // Feature_types
                        if (obj.feature.feature_types)
                            obj.feature.feature_types.forEach(function (x) {
                                addEntry(doc, 'feature_types', x.title);
                                // doc.feature_types.push(x.title);
                                addEntry(doc, 'feature_type_ids', x.id)
                                // doc.feature_type_ids.push(x.id);
                            });

                        // Names:  these will be joined in the index
                        // console.log(JSON.stringify(obj.feature.names, undefined, 2))


                        // NEED TO USE THE names INTERFACE INSTEAD for completeness
                        //obj.feature.names.forEach(function (x) {
                        //    var fieldname = "name_" + x.language + "_" + x.view + "_" + x.writing_system + ((x.orthographic_system) ? "_" + x.orthographic_system : "");
                        //    doc[fieldname] = x.name;
                        //});

                        if (obj.feature.captions) {
                            // Captions
                            // console.log("CAPTIONS:" + JSON.stringify(obj.feature.captions,undefined,2))
                            obj.feature.captions.forEach(function (x) {
                                var lang = x.language;
                                var content = x.content;
                                doc['caption_' + lang] = content;
                            });
                        }

                        if (obj.feature.nested_captions) {
                            obj.feature.nested_captions.forEach(function (x) {
                                var lang = x.language;
                                var content = x.content;
                                doc['caption_' + lang] = content;
                            });
                        }

                        // Summaries
                        // console.log(JSON.stringify(obj.feature.summaries, undefined, 2));
                        obj.feature.summaries.forEach(function (x) {
                            var lang = x.language;
                            var content = x.content;
                            doc['summary_' + lang] = content;
                        });

                        // Descriptions

                        // Descriptions are a mystery.  Ask Andres!

                        // console.log(JSON.stringify(obj.feature.illustrations, undefined, 2));
                        obj.feature.illustrations.forEach(function (x) {
                            var url = x.url;
                            var type = x.type;

                            addEntry(doc, 'illustration_' + type + '_url', url);

                            // doc['illustration_' + type + '_url']=url;
                        });

                        // ANCESTORS!  by PERSPECTIVE
                        if (obj.feature.perspectives) {
                            obj.feature.perspectives.forEach(function (pers) {
                                pers.ancestors.forEach(function (x) {
                                    var ancestor = x.header;
                                    var ancestorid = x.id;

                                    if (!pers) {
                                        pers = {code: null};
                                    }
                                    // console.log("ANCESTOR: " + pers.code + ":" + ancestorid + ":" + ancestor);

                                    addEntry(doc, 'ancestors_' + pers.code, ancestor);
                                    addEntry(doc, 'ancestor_ids_' + pers.code, ancestorid);

                                })
                            });
                        } else {
                            obj.feature.ancestors.forEach(function (x) {
                                var ancestor = x.header;
                                var ancestorid = x.id;

                                var pers = "default";

                                // console.log("ANCESTOR: " + pers+ ":" + ancestorid + ":" + ancestor);

                                addEntry(doc, 'ancestors_' + pers, ancestor);
                                addEntry(doc, 'ancestor_ids_' + pers, ancestorid);

                            });
                        }

                        doc.interactive_map_url = obj.feature.interactive_map_url;
                        doc.kmz_url = obj.feature.kmz_url;
                        doc.created_at = obj.feature.created_at;
                        doc.updated_at = obj.feature.updated_at;
                        doc.has_shapes = Boolean(obj.feature.has_shapes);
                        doc.has_altitudes = Boolean(obj.feature.has_altitudes);
                        if (obj.feature.closest_fid_with_shapes)
                            doc.closest_fid_with_shapes = obj.feature.closest_fid_with_shapes;

                        // collect from the names api (napi)
                        var napi_options = JSON.parse(JSON.stringify(options));
                        napi_options.path = '/features/' + kid + "/names.json",
                            http.request(napi_options, function (napi_res) {
				if (++napi_count > 1) console.log("COUNT NAPI checkout: " + (napi_count));
                                var raw2 = [];
                                napi_res.setEncoding('utf8');

                                napi_res.on('error', function (e) {
                                    conosle.log("BOOFOO");
                                    console.log(e);
                                });

                                napi_res.on('data', function (chunk2) {
                                    raw2.push(chunk2);
                                });

                                napi_res.on('end', function () {
                                    var napi_obj = JSON.parse(raw2.join(''));
                                    //console.log(util.inspect(napi_obj, false, null));

                                    // console.log("TRAVERSING: " + JSON.stringify(napi_obj));
                                    var names = traverse(napi_obj).reduce(function (acc, x) {
                                        if (x && x.name && this.parent.key === "names" && typeof x !== 'string') {
                                            //console.log("============");
                                            //console.dir(x);
                                            //console.log("============");
                                            var joined = "name_"

                                            if (x.language) {
                                                joined += abbreviate(x.language)
                                            }
                                            if (x.relationship) {
                                                if (typeof x.relationship == 'string' || x.relationship.code ) {
                                                    joined += "_" + abbreviate(x.relationship);
                                                }
                                            }
                                            if (x.writing_system) {
                                                joined += "_" + abbreviate(x.writing_system);
                                            }
                                            acc[joined] = x.name;
                                        }
                                        return acc;
                                    }, {});
                                    //console.dir(names);
                                    _.extend(doc, names);
                                    doc.checksum = checksum(doc);
                                    callback(null, doc);
                                    napi_res.resume();
				    if (--napi_count !== 0) console.log("COUNT napi = " + ( napi_count ));
                                });
                            }).end();
                    }
                    catch (err) {
                        console.log("Error: " + err);
                        console.log("Return was: " + raw.join('\n'));
                        callback(null, null);
                    }
                    finally {
                        res.resume();
			if (--kget_count !== 0) console.log("COUNT kget= " + ( kget_count ));
                    }
                });

            }).end();
        } catch (e) {
            console.log(">>>>>>>>>>>>>ERRRORRROROOORRRORROORORORRRRORRROR!>>>>>>>>>>>>>>");
            console.log(e);
            throw e;
        }
    });
}


exports.checkEtag = function (kmapuid, callback) {

    // This is a "universal" kmapid.   Translate!
    var __ret = grokKClass(kmapuid);
    var kclass = __ret.kclass;
    var kid = __ret.kid;
    var pre = Settings.kmaps_prefix;

    var options = {
        host: pre + kclass + Settings.kmaps_domain,
        port: 80,
        path: '/features/' + kid + ".json",
        method: 'HEAD'
    };

    // console.log("trying " + JSON.stringify(options));

    http.request(options, function (res) {

        //console.log("Getting HEAD: " + JSON.stringify(options));
        //console.log("HEADERS: " + JSON.stringify(res.headers));

	if (++etag_count > 1) console.log ("COUNT etag = " + (etag_count));	

        res.on('error', function(e) {
            console.log("CHUCKMUCK");
            console.log(e);
            callback(e);
        });

        res.on('data',function(x){

        });

        res.on('end',function() {
            if (res.headers.etag) {
                callback(null, res.headers.etag);
            } else {
                callback(null, null);
            }
            res.resume();
	    if (--etag_count !== 0) console.log ("COUNT etag = " + (etag_count));	
        });



    }).end();


}

exports.getVersion = function() {
    return VERSION;
}


function addEntry(doc, field, data) {
    // first determine if the field exists

    // console.log("addEntry: " + field + " = " + JSON.stringify(data));


    if (doc[field]) {
        // if it exists and isn't an array convert to an array and re-add value to the array
        if (!_.isArray(doc[field])) {
            var save = doc[field];
            doc[field] = [ save ];
        }

        // push the new data into the array
        doc[field].push(data);
        // console.log("pushed: " + JSON.stringify(data));

    } else {
        // else treat it as a single value
        doc[field] = data;
        // console.log("inserted: " + JSON.stringify(data));
    }

}

exports.getKmapsTree = function (host, callback) {
    var kmaps_options = {
        host: host,
        port: Settings.kmaps_port,
        path: Settings.kmaps_fancy_path,
        method: 'GET'
    };

    // console.log("TRyING: " + JSON.stringify(kmaps_options));

    var raw = [];
    var obj = {};
    http.request(kmaps_options,function (res) {
        try {

	if (++kmap_count > 1) console.log("COUNT kmap checkout: " + (kmap_count));
            res.on('error', function(e) {
                console.log("BORTLES");
                console.log(e);
                callback(e);
            });

            res.setEncoding('utf8');
            res.on('data', function (chunk) {
                // console.log("data: " + chunk);
                raw.push(chunk);
            });

            res.on('end', function () {
                obj = JSON.parse(raw.join(''));
                // console.log("end: " + raw.join(''));
                callback(null, obj);
                res.resume();
		if (--kmap_count !== 0) console.log("COUNT kmap checkin: " + (kmap_count));
            });


        }
        catch (err) {
            console.log("Error: " + err);
            callback(err, null);
        }
    }).end();


}

exports.getKmapsList = function (host, callback) {
    exports.getKmapsTree(host, function (err, obj) {
        var nodes = traverse(obj).reduce(function (acc, x) {
            if (x.key) {
                acc.push(x.key)
            }
            if (x.id) {
                acc.push(x.id)
            }
            ;
            return acc;
        }, []);
        callback(err, nodes);
    });

}


// Use etags (and maybe code version?) to determine staleness.
// Use names api!
// Add "ancestors"
// wonder maybe the harvesting should remain here and the services just notify of changes.


function checksum (obj, algorithm, encoding) {
    return crypto
        .createHash(algorithm || 'sha1')
        .update(JSON.stringify(obj), 'utf8')
        .digest(encoding || 'hex')
}
