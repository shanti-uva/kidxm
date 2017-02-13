/**
 * Created by ys2n on 7/16/14.
 */
// const debug = require('debug')('solrnested_connector_test');


const kmapid_fixtures = require('./fixtures').kmapid_fixtures;
const solrmanager = require('../connectors/solr_manager');
const solrnested = require('../connectors/solr_nested_connector');
const chalk = require('chalk');
const async = require('async');
const log = require('tracer').colorConsole();
const _ = require('underscore');

// init: solr_client
const solr = require('solr-client');
const sourceConfig = {
    'host': 'ss558499-us-east-1-aws.measuredsearch.com',
    'port': 443,
    'secure': true,
    'path': '/solr',
    'core': 'kmterms_dev'
}
var solrclient = solr.createClient(sourceConfig);
const OUTDIR = "./test-outdir";
const FORCEWRITETRUE = true;
const FORCEWRITEFALSE = false;



if (false) {
    exports["addDocument"] = function (test) {
        test.expect(1);
        solrnested.getDocument("places-637", function (err, doc) {
            log.debug(chalk.red("err : " + JSON.stringify(err)));
            log.debug(chalk.red("doc : " + JSON.stringify(doc)));
            solrmanager.addTerms(doc, function () {

                log.info("solrmanager.addDoc()");
                log.info("%d", arguments);

            });
            test.ok(true);
            test.done();
        });
    };
}

if (true) {
    kmapid_fixtures.forEach(
        function (x) {
            exports["testGetDocument-" + x.id] = function (test) {

                process.env.solr_write_user = "solradmin";
                process.env.solr_write_password = "IdskBsk013";
                process.env.solr_write_force = false;
                process.env.solr_write_stalethresh = 360000 * 1000; // (3600 seconds)


                var func = function (x, y) {
                    console.error(JSON.stringify(y, undefined, 2));
                }

                solrnested.getDocument(x.id, func, OUTDIR, FORCEWRITETRUE);

                test.expect(1);

                if (_.isEmpty(x)) {
                    return;
                }

            }
        }
    )
}

var addDocTodSolr = function (err, resp) {
    log.info("Here>:)");
    // log.info("%d",resp.docs);
    log.info(resp.docs.length + " docs.");
    solrmanager.addTerms(resp.docs, function (err, out) {
        log.info("===solrmanager.addDoc()");
        log.info("err: " + err);
        log.info("out: " +
            "" + JSON.stringify(out));
        // log.info("%d",resp);
    });
};


/* */
var worker = function (solrResp, success_cb) {
    //   Expecting a solr JSON document
    //   callback will return a success object to the callback

    log.info("======> %j", solrResp);
    success_cb({success: true});
}


if (false) {

    const POOLSIZE = 1; // number of workers

    kmapid_fixtures.forEach(
        function (x) {
            log.info(x.id);
            exports["testWalk-" + x.id] = function (test) {

                test.expect(1);
                log.info("test");
                var mark = "*";
                var i = 0;


                var testcallback = function () {
                    log("TEST CALLBACK: %j", arguments);
                    test.ok(false);
                    test.done();
                }


                // while (false && i++ < 2) {  // disabled
                //     async.waterfall([
                //         getKmapDocTree
                //     ], addDocsToSolr
                //     );
                // }


                q = async.queue(worker, POOLSIZE);
                getKmapDocTree('*', 'places-433', function (err, docs) {
                    log.warn("pushing to the queue: " + JSON.stringify(docs, undefined, 3));
                    q.push(docs, testcallback);
                });

                q.drain(function () {

                    console.error("I'm so drained!");
                })

                while (q.running > 0) {
                    console.log(".");
                }


            }
        }
    )
}
//     solrnested.getD
// ocument(x.id,
//         function (err, docs) {
//
//             try {
//                 log.debug(chalk.red("=====>  getDocument (" + x.id + ") returned: " + docs.length + " documents."));
//                 docs.forEach(function (d) {
//                     log.debug(d.id);
//                 });
//                 if (false) {
//                     log.info(chalk.blue("###### FINAL OUTPUT ######"));
//                     log.info(chalk.blue(JSON.stringify(docs, undefined, 3)));
//                     log.info(chalk.blue("##########################"));
//                 }
//
//                 solrmanager.addAssets(docs, function () {
//                     log.info("solrmanager.addDoc()");
//                     log.info("%d",arguments);
//                 });
//
//
//                 if (err) {
//                     log.info("%d",err);
//                     log.debug("#########################");
//                 }
//             } catch (err) {
//                 log.info(err);
//             }
//             test.ok(true);
//             test.done();
//         }
//     )
