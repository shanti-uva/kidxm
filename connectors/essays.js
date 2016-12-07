
var http = require('http');
var Settings = {
    essays_host: 'essays.drupal-dev.shanti.virginia.edu',
    essays_path: '/shanti_kmaps_fields/api/node/'
}

exports.getDocument = function (docid, callback) {

    // http://essays.drupal-dev.shanti.virginia.edu/shanti_kmaps_fields/api/node/210
    var options = {
        host: Settings.essays_host,
        port: 80,
        path:  Settings.essays_path + docid,
        method: 'GET'
    };

    var doc = {};

    console.log("Attempting to contact: " + JSON.stringify(options));

    http.request(options,function (res) {
        var raw = [];
        console.log('STATUS: ' + res.statusCode);
        console.log('HEADERS: ' + JSON.stringify(res.headers));
        res.setEncoding('utf8');
        res.on('data', function (chunk) {
            raw.push(chunk);
        });

        res.on('end', function () {
            try {
                var obj = JSON.parse(raw.join(''))[0];
                console.log(JSON.stringify(obj,undefined,2));

                if (res.headers.etag) { doc.etag = res.headers.etag }
                doc.kmapid = obj.kmapid;
                doc.url = obj.url;
                doc.bundle = obj.bundle;
                doc.summary = obj.summary;
                doc.caption = obj.caption;
                doc.id = obj.id;
                doc.service = obj.service;
                doc.uid = obj.uid;
                doc.thumbnail_url = obj.thumbnail_url;
                callback(null,doc);
            }
            catch(err) {
                console.log("Error: " + err );
                callback(err,null);
            }
        })
    }).end();





};

exports.getItemIdList = function (docidlist, callback) {

};

exports.getDocumentsByKmapId = function (kmapid, callback) {

};

exports.getDocumentsByKMapIdStale = function (kmapid, staletime, callback) {

};
