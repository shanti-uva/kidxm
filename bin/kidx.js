#!/usr/bin/env node

/**
 *
 * kidx commandline executor
 *
 * Created by ys2n on 11/28/16.
 */
var pkg = require('../package');
var prog = require('commander');
var solr = require('solr-client');
var extend = require('extend');

const JSON = require('circular-json');
const defaults = {
    config: null,
    kmapsServer: null,
    solrServer: null,
    filterQuery: null,
    verbose: false,
    clear: false
}

prog.version(pkg.version);


var parseOptions = function (options) {

    var newOpts = {
        kmapsServer: options.parent.kmapsServer,
        solrServer: options.parent.solrServer,
        verbose: options.parent.verbose,
        filterQuery: options.filterQuery,
        clear: options.clear
    };

    var opts = extend(true,{},defaults);

    if (opts.verbose) {
        console.log("Verbose selected!");
    }

    if (opts.kmapsServer) {
        console.log("kmapsServer = " + opts.kmapsServer);
    }

    if (opts.solrServer) {
        console.log("solrServer = " + opts.solrServer);
    }

    if (opts.filterQuery) {
        console.log("filterQuery = " + opts.filterQuery);
    }

    if (opts.clean) {
        console.log("clean = " + opts.clean);
    }

    return newOpts;

};

prog
    .option('-f --config <file>', "use the JSON file for default configurations.")
    .option('-k --kmaps-server <kmapsBaseUrl>', "specify the kmapsBaseUrl.")
    .option('-s --solr-server <solrBaseUrl>', "specify the solrBaseUrl.")
    .option('-v --verbose', "verbose output");

prog.command("populate <domain>")
    .alias("pop")
    .description("populate the <domain>")
    .option('-C, --clear', 'clear index (for domain) first')
    .option('-q, --filter-query <filterQuery>', 'SOLR filter query')
    .action(function (domain, options) {
        console.log("Running Populate for " + domain + "...");

        // kick off the population of a domain....

    });

// show help
prog.command("help")
    .description("show help")
    .action(function () {
        prog.help();
    });

prog.command("*")
    .action(function (x,y,z) {
	console.dir ("x = %s", x);
	prog.help();
    });

// main
prog.parse(process.argv);

// fallback for no arguments
if (!prog.args.length){ prog.help(); }


console.dir(prog.args);




