#!/usr/bin/env node

/**
 *
 * kidx commandline executor
 *
 * Created by ys2n on 11/28/16.
 */
var log = require('tracer').colorConsole({level:'warn'}) ;

const pkg = require('../package');
const prog = require('commander');
const solr = require('solr-client');
const extend = require('extend');
var async = require('async');
var solrmanager = require('../connectors/solr_manager');
var getDocument = require('../connectors/solr_nested_connector').getDocument;
const _ = require("underscore");
const POOLSIZE = 5;
var term_index_options = {
    // host : host,
    // port : port,
    // core : core,
    // path : path,
    // agent : agent,
    // secure : secure,
    // bigint : bigint,
    // solrVersion: solrVersion
    //

    // https://ss558499-us-east-1-aws.measuredsearch.com/solr/kmterms_dev
    'host': 'ss558499-us-east-1-aws.measuredsearch.com',
    'port': 443,
    'path': '/solr',
    'secure': true,
    'core': 'kmterms_dev'
};

//  Might want different SOLR WRITE AND READ CLIENTS

var solrclient = solr.createClient(term_index_options);
solrclient.basicAuth("solradmin","");  // TODO: REFACTOR


const JSON = require('circular-json');
const defaults = {
    config: null,
    kmapsServer: null,
    solrServer: null,
    filterQuery: null,
    verbose: false,
    clear: false
};

prog.version(pkg.version);
var parseOptions = function (options) {

    var newOpts = {
        kmapsServer: options.parent.kmapsServer,
        solrServer: options.parent.solrServer,
        verbose: options.parent.verbose,
        filterQuery: options.filterQuery,
        clear: options.clear
    };

    var opts = extend(true, {}, defaults);

    if (opts.verbose) {
        log.debug("Verbose selected!");
    }

    if (opts.kmapsServer) {
        log.debug("kmapsServer = " + opts.kmapsServer);
    }

    if (opts.solrServer) {
        log.debug("solrServer = " + opts.solrServer);
    }

    if (opts.filterQuery) {
        log.debug("filterQuery = " + opts.filterQuery);
    }

    if (opts.clean) {
        log.debug("clean = " + opts.clean);
    }

    return newOpts;

};

var getKmapDocTree = function (uid, cb) {
    var s = uid.split('-');
    var type = s[0];
    var id = s[1];
    log.info(">>>>>>>  kmapid = " + uid + " type = " + type + " id= " + id);

    var query = solrclient.createQuery()
        .q("ancestor_ids_generic:" + id)
        .fl('id', '_timestamp_')
        .matchFilter("tree", type)
        .sort({'ancestor_id_path': 'desc', 'id': 'desc'})
        .rows(8000);
    log.info(query);

    solrclient.basicAuth("solradmin","");  // TODO: REFACTOR
    solrclient.search(query, function (e, fullResp) {
        log.info("callback from solr-client.search");
        if (e) {
            log.debug("Error from solrclient: " + JSON.stringify(solrclient.options));
            log.error("ERROR:" + e);
            cb(e);
        } else {
            log.error("%j",fullResp);
            // log.info("NUMNUM: " + oo.response.numFound);
            // log.info("next cursorMark: " + fullResp.nextCursorMark);
            // log.info("Returning:  " + fullResp.response.docs.length);
            cb(null, fullResp);
        }
    });
};


var addDocsToSolr = function (docs, callback) {
    log.debug(docs.length + " docs.");
    log.debug(JSON.stringify(docs,undefined,3));
    log.info(_.map(docs, function(doc) { return doc.uid + " (" + doc.header +"):    " + doc.ancestors.join("/")}));

    solrmanager.addTerms(docs, function (err, out) {
        // log.info("out: " +
        //     "" + JSON.stringify(out));
        // log.info("%d",resp);
        callback(err,out);
    });
};

process.on('uncaughtException', function (err) {
    log.error(err.message);
    log.error(err.stack);
    log.log("Node NOT Exiting...");
});



prog
    .option('-f --config <file>', "use the JSON file for default configurations.")
    .option('-k --kmaps-server <kmapsBaseUrl>', "specify the kmapsBaseUrl.")
    .option('-s --solr-server <solrBaseUrl>', "specify the solrBaseUrl.")
    .option('-v --verbose', "verbose output")

prog.command("populate <start>")
    .alias("pop")
    .description("populate starting from  <start>")
    .option('-C, --clear', 'clear index (for domain) first')
    .option('-q, --filter-query <filterQuery>', 'SOLR filter query')
    .action(function (domain, options) {
        log.info("Running Populate for " + domain + "...");

        getKmapDocTree(domain, function (err, resp) {
            var docs = resp.response.docs;

            var munge = function (doc, bc) {

                // var ts = Date.parse(doc._timestamp_);
                var now = new Date().getTime();

                // log.error("ts  = " + ts);
                // log.error("now = " + now);

                async.waterfall(
                    [
                        async.apply(getDocument, doc.id),
                        addDocsToSolr
                    ],
                    function(ret) {
                        log.debug("RUNNING POSTPROCESS");
                        bc(null);
                        // log.debug(JSON.stringify(arguments));
                    }
                );
            }

            log.debug('DOCS:    %j', docs);

            async.eachLimit(docs, POOLSIZE, munge, function (err, result) {
                if (err) {
                    log.error(err);
                }
                if (result) {
                    log.error(" ===> %j", result);
                }
            });


            // for (var i=0; i < 5; i++ ) {
            //     var x = docs[i];
            //     log.info(x.id);
            //     getdoc(x.id, function () {
            //         log.info('arguments: %j', arguments);
            //     });
            // }
        });


    });


// show help
prog.command("help")
    .description("show help")
    .action(function () {
        prog.help();
    });

prog.command("*")
    .action(function (x, y, z) {
        log.info("x = %s", x);
        prog.help();
    });

// main
prog.parse(process.argv);

// fallback for no arguments
if (!prog.args.length) {
    prog.help();
}

JSON.stringify(prog.args, undefined, 3);




