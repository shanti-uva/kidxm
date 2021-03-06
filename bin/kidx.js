#!/usr/bin/env node

// TODO: refactor settings so you can pass these on the command line!
process.env.solr_write_user = "solradmin";
process.env.solr_write_password = "IdskBsk013";

// process.env.solr_write_user = "solrprod";
// process.env.solr_write_password = "QiscMU5ho2q";

// process.env.solr_write_force = true;
process.env.solr_write_stalethresh = 10000 * 3600 * 1000; // (3600 * 1000 ms = 1 hour)
process.env.solr_log_level = process.env.solr_log_level || 'warn';
process.env.solr_local_outdir = "./output";

const UPDATE_POOLSIZE = 1;
const CLEAN_POOLSIZE = 5;

// CONFIGS
const target_index_options = {
    // https://ss558499-us-east-1-aws.measuredsearch.com/solr/kmterms_dev
    'host': 'ss558499-us-east-1-aws.measuredsearch.com',
    'port': 443,
    'path': '/solr',
    'secure': true,
    'core': 'kmterms_dev'
};

 // const source_index_options = {
 // // host : host, port : port, core : core, path : path, agent : agent, secure : secure, bigint : bigint, solrVersion: solrVersion
 // // https://ss558499-us-east-1-aws.measuredsearch.com/solr/kmterms_dev
 // 'host': 'ss206212-us-east-1-aws.measuredsearch.com',
 // 'port': 443,
 // 'path': '/solr',
 // 'secure': true,
 // 'core': 'kmterms'
 // };

const source_index_options = target_index_options;

const default_options = {
    config: null,
    kmapsServer: null,
    solrServer: null,
    filterQuery: null,
    singleMode: false,
    timings: false,
    sampleCount: 0,
    reverse: false,
    verbose: false,
    quiet: false,
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


// require("longjohn");
const JSON = require('circular-json');
const pkg = require('../package');
const command = require('commander');
const solr = require('solr-client');
const extend = require('extend');
const async = require('async');
const _ = require("lodash");

const TIMEOUT = 600 * 1000; // in millisecs
const solrmanager = require('../connectors/solr_manager');
const rawGetNestedDocument = async.timeout(require('../connectors/solr_nested_connector').getDocument, TIMEOUT);

// Caching
const cacheManager = require("cache-manager");
const fsStore = require("cache-manager-fs-binary");
const Events = require('events');

// Since this is a "script" let's create a private scope for ourselves.
;(function () {

// Initialize solr clients.
    var solr_read_client = solr.createClient(source_index_options);
    var solr_write_client = solr.createClient(target_index_options);
    var diskcache;

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

    //
    // Caching
    //
    var cacheEventEmitter = new Events.EventEmitter();
    cacheEventEmitter.ready = false;

    // If the event was already fired, fire it for any new listeners
    cacheEventEmitter.on('newListener', function newCacheListener() {
        log.warn("newListener arguments: %j", arguments);
        log.warn("newListener registered. ready = " + this.ready);
        if (this.ready) {
            this.emit('cacheReady')
        } else {
            log.error("Waiting for cache to be ready...");
        }
    });

    // When the cacheReady event is fired, set the ready flag, to avoid lost event.
    cacheEventEmitter.on('cacheReady', function cacheReady() {
        this.ready = true;
        log.warn("Cache is ready!  ready = " + this.ready);
    })

    var initCache = function() {
        log.warn("Initializing cache...");
        this.diskCache = cacheManager.caching({
            store: fsStore,
            options: {
                preventfill: false,
                reviveBuffers: true,
                binaryAsStream: false,
                ttl: 120 * 60 * 60 * 24 /* seconds */,
                maxsize: 500000 * 1000 * 1000 * 1000/* max size in bytes on disk */,
                path: 'diskcache',
                fillcallback: function () {
                    log.warn("Cache ready!");
                    cacheEventEmitter.emit('cacheReady');
                }
            }
        });
    }

    var getNestedDocument = function (uid,force,callback) {
        log.error("******** getNestedDocument [ %s ] (force = %s callback = %s)", uid, force, typeof callback);
        if (force) {
            rawGetNestedDocument(uid, callback);
        } else {
            diskCache.wrap(uid, function (cacheCallback) {
                log.info("******** [ %s ] retrieving raw document...", uid);
                rawGetNestedDocument(uid, cacheCallback);
            }, callback);
        }
    }

    var populate = function (domain, opts) {
        // log.info("Running Populate for %s with options %s", domain, JSON.stringify(dddddd, undefined, 2));
        log.warn("Running Populate for %s", domain);
        initCache();
        var executeJob = function () {
            getKmapDocTree(domain, opts.singleMode, function (err, docs) {
                // wrap rawGetNestedDocument() in cache
                var sortvalue = function (x) {
                    var s = x.level_i + "/" + x.id + "/" + x.ancestor_id_path;
                    return s;
                }
                var uniq_docs = _.uniq(_.sortBy(docs, sortvalue), false, sortvalue); // Use path for uniqueness
                if (opts.reverse) {
                    log.error("Reversing list");
                    uniq_docs = uniq_docs.reverse();
                }

                if (!opts.quiet) {
                    log.warn('number of DOCS: %d', uniq_docs.length);
                }
                var total_count = uniq_docs.length;

                var munge = function (document, index, munge_callback) {
                    if (!opts.quiet) {
                        log.warn("Starting %s ( %d/%d ) %j %s", document.id, index + 1, total_count, document.ancestor_id_path, document.header);
                    }
                    if (opts.timings) {
                        console.time("munge:" + document.id);
                    }
                    if (opts.sampleCount && index % opts.sampleCount === 0) {
                        var sampleName = "MUNGE-SAMPLE-" + (parseInt(index / opts.sampleCount) + 1);
                        log.error("TIMER: " + sampleName);
                        console.time(sampleName);
                    }
                    if (opts.sampleCount && index !== 0 && index % opts.sampleCount === 0) {
                        console.timeEnd("MUNGE-SAMPLE-" + parseInt(index / opts.sampleCount));
                    }


                    log.error("opts.force=" + opts.force);
                    async.waterfall(
                        [
                            async.apply(getNestedDocument, document.id, opts.force),
                            addDocsToSolr
                        ],
                        function post(err, ret) {
                            log.debug("RUNNING POSTPROCESS");
                            if (!opts.quiet && !err) {
                                log.warn("Processed %s ( %d/%d ) %j %s", document.id, index + 1, total_count, document.ancestor_id_path, document.header);
                            }
                            else {
                                log.error("Error: [ %s ] %s", document.id, err);
                            }
                            munge_callback(null, index);
                            // munge_callback(err,index);
                            if (opts.timings) {
                                console.timeEnd("munge:" + document.id);
                            }
                        }
                    );
                }

                async.eachOfLimit(uniq_docs, UPDATE_POOLSIZE, munge, function (err, result) {
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

        cacheEventEmitter.on('cacheReady', function() {
            executeJob();
        })

    };

    var clean = function (domain, opts) {
        log.warn("Running Clean for %s", domain);

        var executeJob = function () {
            getKmapDocTree(domain, opts.singleMode, function (err, docs) {
                // wrap rawGetNestedDocument() in cache
                var sortvalue = function (x) {
                    var s = x.level_i + "/" + x.id + "/" + x.ancestor_id_path;
                    return s;
                }
                var uniq_docs = _.uniq(_.sortBy(docs, sortvalue), false, sortvalue); // Use path for uniqueness
                if (opts.reverse) {
                    log.error("Reversing list");
                    uniq_docs = uniq_docs.reverse();
                }

                log.error('number of DOCS: %d', uniq_docs.length);

                var total_count = uniq_docs.length;

                var scrub = function (document, index, scrub_callback) {
                    if (!opts.quiet) log.warn("Cleaning %s ( %d/%d ) %j %s", document.id, index + 1, total_count, document.ancestor_id_path, document.header);

                    var uid = document.id;
                    if (opts.sampleCount && index % opts.sampleCount === 0) {
                        var sampleName = "CLEAN-SAMPLE-" + (parseInt(index / opts.sampleCount) + 1);
                        log.error("TIMER: " + sampleName);
                        console.time(sampleName);
                    }
                    if (opts.sampleCount && index !== 0 && index % opts.sampleCount === 0) {
                        console.timeEnd("CLEAN-SAMPLE-" + parseInt(index / opts.sampleCount));
                    }
                    if (opts.timings) {
                        console.time(uid)
                    }
                    ;
                    var query = solr_write_client.createQuery()
                        .q("id:" + uid)
                        .fl('id,level_i,_timestamp_,block_type,ancestor_id_path,header')
                        .sort({'level_i': 'asc', 'ancestor_id_path': 'asc', 'id': 'asc'})
                        .rows(10);

                    solr_write_client.search(query, function (e, fullResp) {
                        // log.error(query);
                        log.debug("callback from solr-client.search");
                        if (e) {
                            log.debug("Error from solr_client: " + JSON.stringify(solr_read_client.options));
                            log.error("ERROR:" + e);
                            scrub_callback(e);
                        } else {
                            var count = fullResp.response.docs.length;
                            log.info("count: " + count);
                            if (count == 0) {
                                log.error("No Entry  (" + count + ") for " + uid);
                            } else if (count > 1) {
                                log.error("[ %s ] ( %d/%d ) Too Many Entries  (%d)", uid, index + 1, total_count, count);
                                var deleteQ = "id:" + uid + " AND NOT block_type:parent";
                                solr_write_client.deleteByQuery(deleteQ,
                                    {commitWithin: 20, overwrite: true},
                                    function (e, gronk) {
                                        if (e) {
                                            log.error("[ %s ] ( %d/%d ) Delete failed for \"%s\" : %j", uid, index + 1, total_count, deleteQ, e);
                                        }
                                        else {
                                            log.warn("[ %s ] ( %d/%d ) Delete succesfully executed. Returned: %j", uid, index + 1, total_count, gronk);
                                        }
                                    });
                            } else if (fullResp.response.docs[0].block_type !== "parent") {
                                log.error("[ %s ] solr entry does not have block_type field! %j", uid, fullResp.response.docs[0]);
                            }
                            // log.error("%j", fullResp.response.docs);
                            scrub_callback(null, fullResp.response.docs);
                            if (opts.timings) {
                                console.timeEnd(uid)
                            }
                            ;
                        }
                    }).on('error',
                        function (x, y) {
                            log.error("BOINGO!");
                            log.error(x, y);
                            if (opts.timings) {
                                console.timeEnd(uid);
                            }
                            process.exit(1);
                        });
                }

                async.eachOfLimit(uniq_docs, CLEAN_POOLSIZE, scrub, function (err, result) {
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
        executeJob();

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
            .rangeFilter({field: "-block_type", start: "\"\"", end: "*"})
            .sort({'level_i': 'asc', 'ancestor_id_path': 'asc', 'id': 'asc'})
            .rows(40000);
        var single_query = solr_read_client.createQuery()
            .q("uid:" + uid)
            .fl('id,level_i,_timestamp_,block_type,ancestor_id_path,header')
            .matchFilter("tree", type)
            .sort({'level_i': 'asc', 'ancestor_id_path': 'asc', 'id': 'asc'})
            .rows(40000);
        var query = (singleMode) ? single_query : ancestors_query;
        log.info("singleMode:  %j", singleMode);
        log.info("query: %j", query);


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


        var force = true;

        log.info("ARGUMENTS = %j", arguments);
        log.info("calling AddDocsToSolr with opts = %j  callback = %j", force, callback);
        if (docs && docs.length) {
            log.error("Adding " + docs.length + " docs.");
            log.debug(JSON.stringify(docs, undefined, 3));
            log.debug("addTerms: %j", docs);

            delete docs[0]['_timestamp_'];
            // delete docs[0]['_version_'];

            var doc = docs[0];

            log.warn("Adding %s %j %s to %j", doc.id, doc.ancestor_id_path, doc.header, target_index_options);

            var commit=true; // do we need this?  Are we ever going to not commit or not set autoCommit?
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
            }).on('error', function (x) {
                console.error("Error while writing to Solr for " + doc.id, x);
                process.exit(1)
            });
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
            quiet: options.parent.quiet,
            filterQuery: options.filterQuery,
            force: options.force,
            zanzibar: null,
            poodlebutt: undefined,
            singleMode: options.singleMode,
            timings: options.timings,
            sampleTime: options.sampleTime,
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


        log.error("OPTIONS!" + JSON.stringify(opts, undefined, 2));


        return opts;


    };
//
// MAIN  -- Set up commandline utility
//
    process.on('uncaughtException', function (err) {
        log.error("UNCAUGHT EXCEPTION: " + err);
        log.error(JSON.stringify(err, undefined, 2));
        log.error(err.stack);

        if (err) {
            process.exit(1);
        }
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
        .option('-q --quiet', "quiet output")
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
        .option('-t --timings', "Show timings for each remote call")
        .option('-c --sampleCount <sampleCount>', "Show timings for <sampleCount> samples")
        .action(populate);

    // Setup clean command
    command.command("clean <start-kmapid>")
        .description("clean starting from <start-kmapid>")
        .option('-S --singleMode', "Don't get related kmapids")
        .option('-t --timings', "Show timings for each remote call")
        .option('-c --sampleCount <sampleCount>', "Show timings for <sampleCount> samples")
        .action(clean);

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
