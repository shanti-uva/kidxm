#!/usr/bin/env node

// TODO: refactor settings so you can pass these on the command line!
process.env.solr_write_user = "solradmin";
process.env.solr_write_password = "IdskBsk013";
process.env.solr_write_force = false;
process.env.solr_write_stalethresh = 360000 * 1000; // (3600 seconds)

/**
 *
 * kidx commandline executor
 *
 * Created by ys2n on 11/28/16.
 */
var log = require('tracer').colorConsole({level: 'warn'});
const pkg = require('../package');
const prog = require('commander');
const solr = require('solr-client');
const extend = require('extend');
var async = require('async');
var solrmanager = require('../connectors/solr_manager');
var getDocument = require('../connectors/solr_nested_connector').getDocument;
const _ = require("underscore");
const POOLSIZE = 10;

var term_index_options = {
    // host : host, port : port, core : core, path : path, agent : agent, secure : secure, bigint : bigint, solrVersion: solrVersion
    // https://ss558499-us-east-1-aws.measuredsearch.com/solr/kmterms_dev
    'host': 'ss558499-us-east-1-aws.measuredsearch.com',
    'port': 443,
    'path': '/solr',
    'secure': true,
    'core': 'kmterms_dev'
};

//  TODO: Might want different SOLR WRITE AND READ CLIENTS

var solrclient = solr.createClient(term_index_options);

if (process.env.solr_write_user) {
    log.info("process.env.solr_write_user: " + process.env.solr_write_user);
} else {
    log.warn("process.env.solr_write_user is not set!");
}

if (process.env.solr_write_password) {
    log.info("process.env.solr_write_password: ( length: %s )", process.env.solr_write_password.length);
} else {
    log.warn("process.env.solr_write_password is not set!");
}
solrclient.basicAuth(process.env.solr_write_user, process.env.solr_write_password);  // TODO: REFACTOR

const JSON = require('circular-json');

// defaults for option flags
const defaults = {
    config: null,
    kmapsServer: null,
    solrServer: null,
    filterQuery: null,
    verbose: false,
    force: false,
    clear: false
};

prog.version(pkg.version);

var parseOptions = function (options) {

    var newOpts = {
        kmapsServer: options.parent.kmapsServer,
        solrServer: options.parent.solrServer,
        verbose: options.parent.verbose,
        filterQuery: options.filterQuery,
        force: options.force,
        zanzibar: null,
        poodlebutt: undefined,
        clear: options.clear
    };
    var opts = extend(true, defaults, newOpts);

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

    if (opts.solruser) {
        log.debug("solrUser = " + opts.solrUser);
    }

    if (opts.kmapsServer) {
    }

    if (opts.one) {
        log.info("one = " + opts.one);
    }

    if (opts.force) {
        log.info("force = " + opts.force);
    }

    if (opts.delete) {
        log.info("delete = " + opts.delete)
    }

    if (opts.clear) {
        log.info("clear = " + opts.clear);
    }

    log.info("The final set options: %j", opts);
    return opts;

};

var getKmapDocTree = function (uid, cb) {
    var s = uid.split('-');
    var type = s[0];
    var id = s[1];
    log.info(">>>>>>>  kmapid = " + uid + " type = " + type + " id= " + id);


    //  TODO:  This should page instead of taking 40000 at a time!

    var query = solrclient.createQuery()
        .q("ancestor_ids_generic:" + id)
        .fl('id,level_i,_timestamp_,block_type,ancestor_id_path,header')
        .matchFilter("tree", type)
        .sort({'level_i': 'asc', 'ancestor_id_path': 'asc', 'id': 'asc'})
        .rows(40000);
    log.warn(query);

    solrclient.basicAuth(process.env.solr_write_user, process.env.solr_write_password);  // TODO: REFACTOR
    solrclient.search(query, function (e, fullResp) {
        log.info('%j', fullResp.response.docs);

        log.info("callback from solr-client.search");
        if (e) {
            log.debug("Error from solrclient: " + JSON.stringify(solrclient.options));
            log.error("ERROR:" + e);
            cb(e)
        } else {
            log.error("relateds:" + fullResp.response.docs.length);
            // log.error("%j", fullResp.response.docs);
            cb(null, fullResp);
        }
    });
};

var addDocsToSolr = function (docs, cb) {
    if (docs && docs.length) {
        log.info("Adding " + docs.length + " docs.");
        log.info(JSON.stringify(docs, undefined, 3));
        log.info("addTerms: %j", docs);

        async.series([
                function (cb1) {
                    log.info("Removing: " + docs[0].id);
                    solrmanager.removeTerm(docs[0].id, function (err, out) {
                        log.info("removed old term:  ERR: %j      OUT: %j", err, out);
                        cb1(err,out);
                    });
                },
                function (cb2) {
                    log.info("Adding: " + docs[0].id);
                    solrmanager.addTerms(docs, function (err, out) {
                        log.info("added Terms:  ERR: %j      OUT: %j", err, out);
                        cb2(err, out);
                    });
                }
            ],
            function (err, results) {


                log.debug ("FINAL ERR: %j  RESULTS: %j", err, results);

                cb(err, results);
            }
        )
    } else {
        log.info("Doclist is empty.");
        cb(null, {message: "nuttin\' doin\'"});
    }
}

var addDeleteDoc = function (docs, callback) {
    // log.error('%j', docs);
    callback(null, docs);
};

process.on('uncaughtException', function (err) {
    log.error(err.stack);
    log.log("Node NOT Exiting...");
});


prog
    .option('-h -help')
    .option('-f --config <file>', "use the JSON file for default configurations.")
    .option('-k --kmaps-server <kmapsBaseUrl>', "specify the kmapsBaseUrl.")
    .option('-s --solr-server <solrBaseUrl>', "specify the solrBaseUrl.")
    .option('-v --verbose', "verbose output")
    .option('-u --solr-user', "solr username")
    .option('-p --solr-password', "solr user password")

prog.command("populate <start-kmapid>")
    .alias("pop")
    .description("populate starting from <start-kmapid>")
    .option('-1 --one', "Just do this one.  Do not recurse to through children.")
    .option('-F --force', "force updates.  Ignoring usual update checks.")
    .option('-d --delete', "delete existing entries first.")
    .option('-C, --clear', 'clear index (for domain) first')
    .option('-q, --filter-query <filterQuery>', 'SOLR filter query')
    .action(function (domain, options) {
        var opts = parseOptions(options);

        process.env.solr_write_force = opts.force;

        log.info("Running Populate for %s with options %s", domain, JSON.stringify(opts, undefined, 2));

        getKmapDocTree(domain, function (err, resp) {
            var docs = resp.response.docs;

            // console.dir (docs);

            var total_count = docs.length;

            var munge = function (doc, index, bc) {
                // var ts = Date.parse(doc._timestamp_);
                var now = new Date().getTime();
                async.waterfall(
                    [
                        async.apply(getDocument, doc.id),
                        addDocsToSolr
                    ],
                    function (err,ret) {
                        log.debug("RUNNING POSTPROCESS");
                        log.warn("Processed %s ( %d/%d ) %s %j",doc.id,index,total_count,doc.ancestor_id_path,doc.header);
                        bc (null, index);
                        // bc(err,index);
                    }
                );
            }

            var uniq_docs = _.uniq(docs, false, function (x) {
                // console.dir(x);



                // console.log(x.id + "    " + x.ancestor_id_path);
                return x.id + "/" + x.ancestor_id_path;
            });  // Use path for uniqueness
            log.warn('number of DOCS: %d', uniq_docs.length);

            var primaryDoc = {"id": domain, "level_i": 0, ancestor_id_path: "/" };  // fake primary doc so that this one is done first.
            log.warn("DOCS: pushing %j ", primaryDoc);
            uniq_docs.unshift(primaryDoc);

            async.eachOfLimit(uniq_docs, POOLSIZE, munge, function (err, result) {

                log.error("SNARKO! %j", err);
                log.error("BARKO!  %j", result);

                if (err) {
                    log.error("Error", err);
                }
                if (result) {
                    log.warn("RESULT: %j", result);
                }
            });
        });
    })


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

