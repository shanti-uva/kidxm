/**
 * Created by ys2n on 7/16/14.
 */
const debug = require('debug')('solrnested_connector_test');

var solrmanager = require('../connectors/solrmanager');
var solrnested = require('../connectors/solrnested_connector');
var solr = require('solr-client');
var chalk = require('chalk');

kmapid_fixtures = [
    {type: "subjects", id: "subjects-6403", name: "Tibet and Himalayas"},
    {type: "subjects", id: "subjects-2328", name: "By pupose to be achieved"},
    {type: "subjects", id: "subjects-829", name: ""},
    {type: "places", id: "places-13656", name: "United States of America"},
    {type: "places", id: "places-13736", name: ""},
    {type: "places", id: "places-433", name: "Sera Monastery"},
    {type: "places", id: "places-434", name: ""},
    {type: "places", id: "places-2", name: "Tibet Autonomous Region"},
    {type: "places", id: "places-435", name: ""}
];

exports["addDocument"] = function (test) {
    test.expect(1);
    solrnested.getDocument("places-433", function (err, doc) {
        debug(chalk.red("err : " + JSON.stringify(err)));
        debug(chalk.red("doc : " + JSON.stringify(doc)));
        solrmanager.addTerms(doc, function () {

            console.log("solrmanager.addDoc()");
            console.dir(arguments);


        });
        test.ok(true);
        test.done();
    });
};

if (true)
    kmapid_fixtures.forEach(
        function (x) {
            exports["testGetDocument-" + x.id] = function (test) {
                test.expect(1);
                solrnested.getDocument(x.id,
                    function (err, docs) {

                        try {
                            debug(chalk.red("=====>  getDocument (" + x.id + ") returned: " + docs.length + " documents."));
                            docs.forEach(function (d) {
                                debug(d.id);
                                // console.dir(d);
                            });
                            if (false) {
                                console.log(chalk.blue("###### FINAL OUTPUT ######"));
                                console.log(chalk.blue(JSON.stringify(docs, undefined, 3)));
                                console.log(chalk.blue("##########################"));
                            }

                            solrmanager.addDocs(docs, function () {

                                console.log("solrmanager.addDoc()");
                                console.dir(arguments);


                            });


                            if (err) {
                                console.dir(err);
                                debug("#########################");
                            }
                        } catch (err) {
                            console.log(err);
                        }
                        test.ok(true);
                        test.done();
                    }
                );
            }
        }
    )

