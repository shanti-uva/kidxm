/**
 * Created by ys2n on 12/7/16.
 */
const log = require('tracer').colorConsole({level: process.env.solr_log_level});
var debugRelatedPlaces = false;
var _ = require('underscore');

exports.flattenRelatedPlaces = function (kmapid, related_places) {
    log.debug("flattenRelatedPlaces=> %s", JSON.stringify(related_places, undefined, 3));
    var s = kmapid.split("-");
    var kid = s[1];
    var type = s[0];

    log.debug("kmapid = " + kmapid);
    log.debug("kid = " + kid);
    log.debug("flattenRelatedPlaces type = " + type);

    var finalRelatedPlaces = [];

    var ancestorsToPath = function (ancestors) {
        return (_.map(ancestors, function (x) {
            return x.id;
        })).join('/');
    };

    if (!related_places.topic) {
        throw new Error("Unexpected format for related_Places: no 'topic; node found! [ " + kmapid + "]  :" + JSON.stringify(related_places,undefined, 2));
    }

    if (related_places.topic.features) {
        var relatedPlaces = related_places.topic.features;

        // Collect paging info
        var current_page = related_places.topic.current_page;
        var total_pages = related_places.topic.total_pages;
        var per_page = related_places.topic.per_page;
        var total_entries = related_places.topic.total_entries;
        relatedPlaces.forEach(function(relatedPlace,index) {
            log.debug("%j",relatedPlace);
            var flattened = {};
            flattened["id"] = kmapid + "_relatedPlace_" + relatedPlace.id;
            flattened["related_place_path_s"] = ancestorsToPath(relatedPlace.ancestors.related_place);
            flattened["block_child_type"] = "related_place";
            flattened["block_type"] = "child";
            flattened["related_place_title_s"] = relatedPlace.header;
            flattened["related_place_id_i"] = relatedPlace.id;
            flattened["related_place_has_shapes_b"] = relatedPlace.has_shapes;

            // ASSUMPTION:  there is only one feature type listed.
            flattened["related_place_feature_type_s"] = relatedPlace.feature_types[0].title;
            flattened["related_place_feature_type_id_s"] = "places-" + relatedPlace.feature_types[0].id;
            //

            flattened["current_page"] = current_page;
            flattened["total_pages"] = total_pages;
            flattened["per_page"] = per_page;
            flattened["total_entries"] = total_entries;
            flattened["item_index"] = index;
            flattened["index"] = (Number(current_page) - 1) * Number(per_page) + index + 1;
            flattened["remaining_pages"] = Number(total_pages) - Number(current_page);
            finalRelatedPlaces.push(flattened);
        });
    } else {
        throw new Error("NO related_places found: [ " + kmapid + " ] " + JSON.stringify(related_places));
    }

    log.debug("returning %s", JSON.stringify(finalRelatedPlaces, undefined, 2));
    return finalRelatedPlaces;
}

