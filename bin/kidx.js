#!/usr/bin/env node

// TODO: refactor settings so you can pass these on the command line!
process.env.solr_write_user = "solradmin";
process.env.solr_write_password = "IdskBsk013";
// process.env.solr_write_force = true;
process.env.solr_write_stalethresh = 10000 * 3600 * 1000; // (3600 * 1000 ms = 1 hour)
process.env.solr_log_level = process.env.solr_log_level || 'warn';
process.env.solr_local_outdir = "./output";

// CONFIGS
const source_index_options = {
    // host : host, port : port, core : core, path : path, agent : agent, secure : secure, bigint : bigint, solrVersion: solrVersion
    // https://ss558499-us-east-1-aws.measuredsearch.com/solr/kmterms_dev
    'host': 'ss206212-us-east-1-aws.measuredsearch.com',
    'port': 443,
    'path': '/solr',
    'secure': true,
    'core': 'kmterms'
};
const target_index_options = {
    // host : host, port : port, core : core, path : path, agent : agent, secure : secure, bigint : bigint, solrVersion: solrVersion
    // https://ss558499-us-east-1-aws.measuredsearch.com/solr/kmterms_dev
    'host': 'ss558499-us-east-1-aws.measuredsearch.com',
    'port': 443,
    'path': '/solr',
    'secure': true,
    'core': 'kmterms_dev'
};
const default_options = {
    config: null,
    kmapsServer: null,
    solrServer: null,
    filterQuery: null,
    singleMode: false,
    reverse: false,
    verbose: false,
    force: false,
    clear: false,
    logLevel: 'warn'
};
var log = require('tracer').colorConsole({level: process.env.solr_log_level});

/**
 *kidxd
 *
 * kidx commandline executor
 *
 * Created by ys2n on 11/28/16.
 */


require("longjohn");
const JSON = require('circular-json');
const pkg = require('../package');
const command = require('commander');
const solr = require('solr-client');
const extend = require('extend');
const async = require('async');
const _ = require("underscore");

const TIMEOUT = 600 * 1000; // in millisecs
const solrmanager = require('../connectors/solr_manager');
const rawGetNestedDocument = async.timeout(require('../connectors/solr_nested_connector').getDocument,TIMEOUT);
const POOLSIZE = 1;

// Caching
const cacheManager = require("cache-manager");
const fsStore = require("cache-manager-fs-binary");

// Since this is a "script" let's create a private scope for ourselves.
;(function () {
    //
    // Caching
    //

// Initialize solr clients.
    var solr_read_client = solr.createClient(source_index_options);
    var solr_write_client = solr.createClient(target_index_options);

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

    solr_write_client.basicAuth(process.env.solr_write_user, process.env.solr_write_password);




    var populate = function (domain, opts) {
        // log.info("Running Populate for %s with options %s", domain, JSON.stringify(opts, undefined, 2));
        log.info("Running Populate for %s", domain);

        var executeJob = function() {
            log.error("---------->>>>>  IN-MEMORY CACHE REFILLED    <<<<<-------")
            getKmapDocTree(domain, opts.singleMode, function (err, docs) {
                // wrap rawGetNestedDocument() in cache
                var getNestedDocument = function (uid, cb) {
                    log.info("******** getNestedDocument [ %s ]", uid);
                    diskCache.wrap(uid, function (cacheCallback) {
                        log.info("******** [ %s ] retrieving raw document...", uid);
                        rawGetNestedDocument(uid, cacheCallback);
                    }, cb);
                };
                var sortvalue = function (x) {
                    var s = x.level_i + "/" + x.id + "/" + x.ancestor_id_path;
                    return s;
                }
                var uniq_docs = _.uniq(_.sortBy(docs, sortvalue), false, sortvalue); // Use path for uniqueness
                if (opts.reverse) {
                    log.error("Reversing list");
                    uniq_docs = uniq_docs.reverse();
                }

                log.warn('number of DOCS: %d', uniq_docs.length);

                var total_count = uniq_docs.length;

                var munge = function (document, index, munge_callback) {
                    var now = new Date().getTime();
                    log.warn("Starting %s ( %d/%d ) %j %s", document.id, index, total_count, document.ancestor_id_path, document.header);
                    async.waterfall(
                        [
                            async.apply(getNestedDocument, document.id),
                            addDocsToSolr
                        ],
                        function post(err, ret) {
                            log.debug("RUNNING POSTPROCESS");
                            if (!err) {
                                log.warn("Processed %s ( %d/%d ) %j %s", document.id, index, total_count, document.ancestor_id_path, document.header);
                            }
                            else {
                                log.error("Error: [ %s ] %s", document.id, err);
                            }
                            munge_callback(null, index);
                            // munge_callback(err,index);
                        }
                    );
                }

                async.eachOfLimit(uniq_docs, POOLSIZE, munge, function (err, result) {
                    log.error("SNARKO! %j", err);
                    log.error("BARKO!  %j", result);

                    if (err) {
                        log.error("Error", err);
                    }
                    if (result) {
                        log.info("RESULT: %j", result);
                    }
                });
            });
        }

        var diskCache = cacheManager.caching({
            store: fsStore,
            options: {
                preventfill: false,
                reviveBuffers: true,
                binaryAsStream: false,
                ttl: 120 * 60 * 60 * 24 /* seconds */,
                maxsize: 500000 * 1000 * 1000 * 1000/* max size in bytes on disk */,
                path: 'diskcache',
                fillcallback: executeJob
            }
        });
    };

    const getKmapDocTree = function (uid, singleMode, cb) {
        var s = uid.split('-');
        var type = s[0];
        var id = s[1];
        log.debug(">>>>>>>  kmapid = " + uid + " type = " + type + " id= " + id);

        //  TODO:  This should page instead of taking 40000 at a time!

        var ancestors_query = solr_read_client.createQuery()
            .q("ancestor_ids_generic:" + id)
            .fl('id,level_i,_timestamp_,block_type,ancestor_id_path,header')
            .matchFilter("tree", type)
            .sort({'level_i': 'asc', 'ancestor_id_path': 'asc', 'id': 'asc'})
            .rows(40000);
        var single_query = solr_read_client.createQuery()
            .q("uid:" + uid)
            .fl('id,level_i,_timestamp_,block_type,ancestor_id_path,header')
            .matchFilter("tree", type)
            .sort({'level_i': 'asc', 'ancestor_id_path': 'asc', 'id': 'asc'})
            .rows(40000);
        var query = (singleMode) ? single_query : ancestors_query;
        log.warn("singleMode:  %j", singleMode);
        log.warn("query: %j", query);

        // solr_client.basicAuth(process.env.solr_write_user, process.env.solr_write_password);  // TODO: REFACTOR
        solr_read_client.search(query, function (e, fullResp) {

            log.debug("callback from solr-client.search");
            if (e) {
                log.debug("Error from solr_client: " + JSON.stringify(solr_read_client.options));
                log.error("ERROR:" + e);
                cb(e)
            } else {
                log.error("relateds:" + fullResp.response.docs.length);
                // log.error("%j", fullResp.response.docs);
                cb(null, fullResp.response.docs);
            }
        });
    };

    const addDocsToSolr = function (docs, callback) {
        log.info("calling AddDocsToSolr(%j)%s", docs, docs.length);
        if (docs && docs.length) {
            log.info("Adding " + docs.length + " docs.");
            log.debug(JSON.stringify(docs, undefined, 3));
            log.debug("addTerms: %j", docs);

            delete docs[0]['_timestamp_'];
            delete docs[0]['_version_'];

            var doc = docs[0];

            log.warn("Adding %s %j %s to %j", doc.id, doc.ancestor_id_path, doc.header, target_index_options);

            var commit;
            commit = commit || true;
            solr_write_client.autoCommit = true;
            solr_write_client.update(docs, {commitWithin: 20, overwrite: true}, function (err, report) {
                if (err) {
                    log.log(err);
                    callback(err);
                } else {
                    log.info(report);
                    if (commit) {
                        solr_write_client.commit();
                    }
                    callback(null, report);
                }
            }).on('error', function(x) { console.error("BOMB2", x); process.exit(1)});
        } else {
            log.info("Doclist is empty.");
            callback(null, {message: "nuttin\' doin\'"});
        }
    }

    const parseOptions = function (options) {
        var commandlineOpts = {
            logLevel: options.parent.logLevel,
            kmapsServer: options.parent.kmapsServer,
            solrServer: options.parent.solrServer,
            verbose: options.parent.verbose,
            filterQuery: options.filterQuery,
            force: options.force,
            zanzibar: null,
            poodlebutt: undefined,
            singleMode: options.singleMode,
            reverse: options.reverse,
            clear: options.clear
        };

        // override the default_options with the commandline options
        var opts = extend(true, default_options, commandlineOpts);

        if (opts.force) {
            process.env.solr_write_force = opts.force;
        }

        if (opts.verbose) {
            log.debug("Verbose selected!");
            process.env.DEBUG = true;
        }

        if (opts.logLevel) {
            log.warn("Setting Log Level to " + opts.logLevel);
            process.env.solr_log_level = opts.logLevel;
            log = require('tracer').colorConsole({level: process.env.solr_log_level});
        }

        if (opts.singleMode) {
            log.warn("Single Mode = " + opts.singleMode);
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
//
// MAIN  -- Set up commandline utility
//
    process.on('uncaughtException', function (err) {
        log.error(JSON.stringify(err,undefined, 2));
        log.error(err.stack);
        if (err.code === 'ECONNRESET') {
            process.exit(1);
        }
        log.error("Node NOT Exiting...");
    });


    command.version(pkg.version);
    command
        .option('-h -help')
        .option('-f --config <file>', "use the JSON file for default configurations.")
        .option('-k --kmaps-server <kmapsBaseUrl>', "specify the kmapsBaseUrl.")
        .option('-s --solr-server <solrBaseUrl>', "specify the solrBaseUrl.")
        .option('-v --verbose', "verbose output")
        .option('-L --logLevel <logLevel>', "set the logLevel")
        .option('-u --solr-user', "solr username")
        .option('-p --solr-password', "solr user password")


// Setup populate command
    command.command("populate <start-kmapid>")
        .alias("pop")
        .description("populate starting from <start-kmapid>")
        .option('-1 --one', "Just do this one.  Do not recurse to through children.")
        .option('-r --reverse', "Reverse the list of kmapid's")
        .option('-F --force', "force updates.  Ignoring usual update checks.")
        .option('-d --delete', "delete existing entries first.")
        .option('-C, --clear', 'clear index (for domain) first')
        .option('-q, --filter-query <filterQuery>', 'SOLR filter query')
        .option('-S --singleMode', "Don't get related kmapids")
        .action(populate)

    // Setup help command
    command.command("help")
        .description("show help")
        .action(function () {
            command.help();
        });

    // Setup default command
    command.command("*")
        .action(function (x, y, z) {
            log.info("x = %s", x);
            command.help();
        });

    // Startup the commandline
    command.parse(process.argv);


})();
