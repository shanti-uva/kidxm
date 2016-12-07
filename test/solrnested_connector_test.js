/**
 * Created by ys2n on 7/16/14.
 */

var solrnested = require('../connectors/solrnested_connector');
var solr = require('solr-client');
var chalk = require('chalk');

kmapid_fixtures = [
    { type: "subjects", id: "subjects-6403", name: "Tibet and Himalayas" },
    { type: "subjects", id: "subjects-2328", name: "By pupose to be achieved" },
    { type: "places", id: "places-13656", name: "United States of America" },
    { type: "places", id: "places-433", name: "Sera Monastery" },
    { type: "places", id: "places-2", name: "Tibet Autonomous Region" }

]

exports["testInstantiation"] = function(test) {
    solrnested.getDocument("places-43d3", function(err,doc) {
        console.log(chalk.red("err : " + JSON.stringify(err)));
        console.log(chalk.red("doc : " + JSON.stringify(doc)));
    });
    test.ok(true);
    test.done();
}

if (true)
kmapid_fixtures.forEach(
    function(x) {
        exports["testGetDocument-" + x.id] = function(test) {
            test.expect(1);

            solrnested.getDocument(x.id,
                function (err, docs) {

                    try {
                        console.log(chalk.red("=====>  getDocument (" + x.id + ") returned: " + docs.length + " documents."));
                        docs.forEach(function (d) {
                            console.log(d.id);
                            console.dir(d);
                        })
                        if (true) {
                            console.log(chalk.blue("###### FINAL OUTPUT ######"));
                            console.log(chalk.blue(JSON.stringify(docs, undefined, 3)));
                            console.log(chalk.blue("##########################"));
                        }
                        if (err) {
                            console.dir(err);
                            console.log("#########################");
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
);




