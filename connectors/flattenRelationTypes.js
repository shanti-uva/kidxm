/**
 * Created by ys2n on 12/7/16.
 */

var debugRelatedPlaces = false;
var _ = require('underscore');

exports.flattenRelationTypes = function (kid,relation_types) {
    if (false) {
        console.log("flattenRelationTypes=>");
        console.dir(relation_types, {depth: 2, colors: 'true'});
    }

    var finalRelatedPlaces = [];

    var ancestorsToPath = function (ancestors) { return (_.map(ancestors, function (x) { return x.id; })).join('/'); };

    //
    // feature_relation_types / relation_types
    //
    relation_types.forEach(function (relation, y) {
        if (debugRelatedPlaces) {
            console.log("==>RelatedPlace");
            console.log(JSON.stringify(relation, undefined, 3));
        }

        var relatedId = kid + "_" + relation.code;
        var relation_data = {
            "id": relatedId,
            "child_type_s": "relation",
            "related_place_relation_label_s": relation.label,
            "related_place_relation_code_s": relation.code,
        };

        if (debugRelatedPlaces) {
            console.log("=== relation ================");
            console.dir(relation);
            console.log("=== relation data ============");
            console.dir(relation_data);
        }

        //
        // feature_relation_types / relation_types / categories
        //
        for (var i = 0; i < relation.categories.length; i++) {
            var category = relation.categories[i]
            var category_id = relation_data.id + "_" + category.id;

            //
            // feature_relation_types / relation_types / categories / features
            //
            for (var j = 0; j < category.features.length; j++) {
                var feature = category.features[j];

                if (debugRelatedPlaces) {
                    console.log("Iterating features: " + j);
                    console.dir(feature);

                    console.dir("The Cat's Meow:");
                    console.dir(category);
                }
                var flattened = {
                    id: category_id + "_" + feature.id,
                    child_type_s: "related_place",
                    related_place_id_s: "places-" + feature.id,
                    related_place_header_s: feature.header,
                    related_place_path_s: ancestorsToPath(feature.ancestors),
                    related_place_feature_type_s: category.header,
                    related_place_feature_type_id_s: category.id,
                    related_place_feature_type_path_s: ancestorsToPath(category.ancestors.feature_type),
                    related_place_relation_label_s: relation_data.related_place_relation_label_s,
                    related_place_relation_code_s: relation_data.related_place_relation_code_s
                };

                if (debugRelatedPlaces) {
                    console.log("=== flattened related places ===");
                    console.dir(flattened);
                }
            }
        }

        finalRelatedPlaces.push(flattened);
    });
    return finalRelatedPlaces;
}
