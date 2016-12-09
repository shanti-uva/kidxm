/**
 * Created by ys2n on 12/7/16.
 */
const debug = require('debug')('flattenRelationTypes');

var debugRelatedKmaps = false;
var _ = require('underscore');
var traverse = require('traverse');
const isArray = require('util').isArray;

exports.flattenRelationTypes = function (kmapid, relation_types) {
    debug("flattenRelationTypes");

    debug ("kmapid = " + kmapid);

    var s = kmapid.split('-');
    var type = s[0];   // subjects or places.
    var kid = s[1];

    debug("flattenRelationTypes type = " + type);
    debug("kid = " + kid);

    // console.dir(relation_types, {depth: 6});

    var ancestorsToPath = function (ancestors) {
        return (_.map(ancestors, function (x) {
            return x.id;
        })).join('/');
    };

    // traverse(relation_types).reduce(function (acc, node) {
    //     if (!this.isLeaf && node.label && node.code) {
    //         acc.label = node.label;
    //         acc.code = node.code;
    //         // acc.count = node.features.length;
    //     } else if (!this.isLeaf) {
    //
    //     }
    //     if (!this.isLeaf && (node.id) && (node.header) && (node.ancestors)) {
    //         // debug("path: " + ancestorsToPath(node.ancestors));
    //         acc.path = ancestorsToPath(node.ancestors);
    //         for (var p in node) {
    //             if (node.hasOwnProperty(p)) {
    //                 acc[p] = node[p];
    //             }
    //         }
    //         // debug("=============");
    //         // // console.dir(acc, { depth: 4, colors: true});
    //         // debug("=============");
    //     }
    //     return acc;
    // }, {list: []});


    if (true) {
        debug("flattenRelationTypes=>");
        // console.dir(relation_types, {depth: 2, colors: 'true'});
    }

    if (!relation_types) {
        console.error("null relation_types!");
        relation_types = [];
    }

    // extract out the relation_types array
    if (relation_types.feature_relation_types && isArray(relation_types.feature_relation_types)) {
        relation_types = relation_types.feature_relation_types;
    }

    var finalRelatedKmaps = [];
    var child_type = "related_" + type;  // related_places or related_subjects
    //
    // feature_relation_types
    //
    relation_types.forEach(function (relation, y) {
            if (debugRelatedKmaps) {
                debug("==>" + child_type);
                debug(JSON.stringify(relation, undefined, 3));
            }

            var relatedId = kmapid + "_" + relation.code;
            var relation_data = {};

            console.error("RELATED_ID=" + relatedId );

            relation_data["id"] = relatedId;
            relation_data["child_type_s"] = child_type;
            relation_data[child_type + "_relation_label_s"] = relation.label;
            relation_data[child_type + "_relation_code_s"] = relation.code;

            if (debugRelatedKmaps) {
                debug("=== relation ================");
                // console.dir(relation);
                debug("=== relation data ============");
                // console.dir(relation_data);
            }


            //
            // feature_relation_type  / categories
            //

            var categories;

            if (relation.categories) {
                categories = relation.categories;
            } else {
                categories = [{
                    id: null,
                    features: relation.features
                }]
            }

            for (var i = 0; i < categories.length; i++) {
                var category = categories[i]
                var category_id = (category.id !== null) ? relation_data.id + "_" + category.id : relation_data.id;

                //
                // feature_relation_types / relation_types / categories / features
                //
                var features = category.features;

                for (var j = 0; j < features.length; j++) {
                    //
                    // feature_relation_type / category / features
                    //
                    var feature = features[j];

                    if (debugRelatedKmaps) {
                        debug("Iterating features: " + j);
                        // console.dir(feature);

                        // console.dir("The Cat's Meow:");
                        // console.dir(category);
                    }

                    var flattened = {};
                    flattened["id"] = category_id + "_" + feature.id;
                    flattened["child_type_s"] = child_type;
                    flattened[child_type + "_id_s"] = type + "-" + feature.id;
                    flattened[child_type + "_header_s"] = feature.header;
                    flattened[child_type + "_path_s"] = ancestorsToPath(feature.ancestors);
                    if (category.header) {
                        flattened[child_type + "_feature_type_s"] = category.header;
                    }
                    if (category.id !== null) {
                        flattened[child_type + "_feature_type_id_s"] = category.id;
                    }
                    if (category.ancestor) {
                        flattened[child_type + "_feature_type_path_s"] = ancestorsToPath(category.ancestors.feature_type);
                    }

                    flattened[child_type + "_relation_label_s"] = relation_data[child_type + "_relation_label_s"];
                    flattened[child_type + "_relation_code_s"] = relation_data[child_type + "_relation_code_s"];

                    flattened["nest_type"] = "child";

                    if (debugRelatedKmaps) {
                        debug("=== flattened " + child_type + " ===");
                        // console.dir(flattened);
                    }

                    finalRelatedKmaps.push(flattened);

                }
            }

        }
    )
    ;
    return finalRelatedKmaps;
}
