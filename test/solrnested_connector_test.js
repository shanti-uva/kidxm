/**
 * Created by ys2n on 7/16/14.
 */
// const debug = require('debug')('solrnested_connector_test');

const solrmanager = require('../connectors/solr_manager');
const solrnested = require('../connectors/solr_nested_connector');
const chalk = require('chalk');
const async = require('async');
const log = require('tracer').colorConsole();
const _ = require('underscore');

// init: solrclient
const solr = require('solr-client');
const sourceConfig = {
    'host': 'ss558499-us-east-1-aws.measuredsearch.com',
    'port': 443,
    'secure': true,
    'path': '/solr',
    'core': 'kmterms_dev'
}
var solrclient = solr.createClient(sourceConfig);

kmapid_fixtures = [
    {type: "subjects", id: "subjects-6403", name: "Tibet and Himalayas"},
    {type: "subjects", id: "subjects-2328", name: "By pupose to be achieved"},
    {type: "subjects", id: "subjects-829", name: ""},
    {type: "places", id: "places-13656", name: "United States of America"},
    {type: "places", id: "places-13736", name: ""},
    {type: "places", id: "places-433", name: "Sera Monastery"},
    {type: "places", id: "places-434", name: ""},
    {type: "places", id: "places-2", name: "Tibet Autonomous Region"},
    {type: "places", id: "places-435", name: ""},
    {type: "places", id: "places-637", name: "Lhasa"},

];

if (false) {
    exports["addDocument"] = function (test) {
        test.expect(1);
        solrnested.getDocument("places-637", function (err, doc) {
            log.debug(chalk.red("err : " + JSON.stringify(err)));
            log.debug(chalk.red("doc : " + JSON.stringify(doc)));
            solrmanager.addTerms(doc, function () {

                log.info("solrmanager.addDoc()");
                log.info("%d",arguments);

            });
            test.ok(true);
            test.done();
        });
    };
}

if (false) {
    kmapid_fixtures.forEach(
        function (x) {
            exports["testGetDocument-" + x.id] = function (test) {
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

        log.info("======> %j",solrResp);
        success_cb({ success:true });
}





if (true) {

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
                    log("TEST CALLBACK: %j",arguments);
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
                getKmapDocTree('*','places-433', function(err, docs) {
                    log.warn("pushing to the queue: " + JSON.stringify(docs, undefined, 3));
                    q.push(docs,testcallback);
                });

                q.drain(   function() {

                    console.error("I'm so drained!");
                })

                while (q.running > 0) {
                    console.log (".");
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
//                 solrmanager.addDocs(docs, function () {
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
