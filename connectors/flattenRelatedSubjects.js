/**
 * Created by ys2n on 12/7/16.
 */
const log = require('tracer').colorConsole({level: process.env.solr_log_level});
var debugRelatedSubjects = false;
var _ = require('underscore');

exports.flattenRelatedSubjects = function (kmapid, related_subject) {
    log.debug("flattenRelatedSubjects=> %s", JSON.stringify(related_subject, undefined, 3));
    var s = kmapid.split("-");
    var kid = s[1];
    var type = s[0];


    log.debug("kmapid = " + kmapid);
    log.debug("kid = " + kid);
    log.debug("flattenRelatedSubjects type = " + type);

    var finalRelatedSubjects = [];

    var ancestorsToPath = function (ancestors) {
        return (_.map(ancestors, function (x) {
            return x.id;
        })).join('/');
    };

    if (!related_subject.feature) {
        throw new Error("Unexpected format for related_subjects: no 'feature; node found! [ " + kmapid + "]  :" + JSON.stringify(related_subjects,undefined, 2));
    }

    if (related_subject.feature.feature_types) {
        var featureTypes = related_subject.feature.feature_types;

        featureTypes.forEach(function(featureType,y) {
            log.warn("%j",featureType);
            var flattened = {};
            flattened["id"] = kmapid + "_featureType_" + featureType.id;
            flattened["feature_type_path_s"] = ancestorsToPath(featureType.ancestors.feature_type);
            flattened["block_child_type"] = "feature_types";
            flattened["block_type"] = "child";
            flattened["feature_type_title_s"] = featureType.title;
            flattened["feature_type_id_i"] = featureType.id;
            flattened["feature_type_caption_s"] = featureType.caption;
            finalRelatedSubjects.push(flattened);
        });
    } else {
        log.error("NO feature_types found: [ %s ] %j )", kmapid, related_subject);
    }

    if (related_subject.feature.category_features) {
        var relatedSubjects = related_subject.feature.category_features;
        relatedSubjects.forEach(function (relatedSubject, y) {
            var flattened = {};
            flattened["id"] = kmapid + "_relatedSubject_" + relatedSubject.category.id;
            // flattened["uid"] = kmapid + "_relatedSubject_" + relatedSubject.category.id;
            flattened["related_subject_uid_s"] = "subjects-" + relatedSubject.category.id;
            flattened["block_child_type"] = "related_subjects";
            flattened["block_type"] = "child";
            flattened["related_subject_title_s"] = relatedSubject.category.title;
            flattened["related_subject_category_id_i"] = relatedSubject.category.id;
            flattened["related_subject_caption_s"] = relatedSubject.category.caption;
            flattened["related_subject_label_s"] = relatedSubject.label;
            flattened["related_subject_prefix_label_b"] = relatedSubject.prefix_label;
            flattened["related_subject_parent_show_b"] = relatedSubject.show_parent;
            flattened["related_subject_parent_title_s"] = relatedSubject.parent.title;
            flattened["related_subject_root_show_b"] = relatedSubject.show_root;
            flattened["related_subject_display_string_s"] = relatedSubject.display_string;
            if (relatedSubject.time_units.date)
                flattened["related_subject_time_date_s"] = relatedSubject.time_units.date;
            flattened["related_subject_time_is_range_b"] = (relatedSubject.time_units.is_range === true);
            if (relatedSubject.time_units.is_range)
                flattened["related_subject_time_range_start_s"] = relatedSubject.time_units.start_date;
            if (relatedSubject.time_units.is_range)
                flattened["related_subject_time_range_end_s"] = relatedSubject.time_units.end_date;
            // flattened["related_subject_path_s"] = ancestorsToPath(relatedSubject.ancestors);
            finalRelatedSubjects.push(flattened);
        });
    } else {
        log.error ("No category_features! [ %s ] %j", kmapid, related_subject);
    }

    log.info("returning %s", JSON.stringify(finalRelatedSubjects, undefined, 2));
    return finalRelatedSubjects;
}

