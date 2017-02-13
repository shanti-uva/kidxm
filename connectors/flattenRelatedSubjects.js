/**
 * Created by ys2n on 12/7/16.
 */
'use strict'
const log = require('tracer').colorConsole({level: process.env.solr_log_level||'warn'});
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
            log.debug("%j",featureType);
            var flattened = {};
            flattened["id"] = kmapid + "_featureType_" + featureType.id;
            flattened["feature_type_path_s"] = ancestorsToPath(featureType.ancestors.feature_type);
            flattened["block_child_type"] = "feature_types";
            flattened["block_type"] = "child";
            flattened["feature_type_title_s"] = featureType.title;
            flattened["related_title_s"] = featureType.title;
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
            log.debug("Parsing Related Subject: %s",JSON.stringify(relatedSubject, undefined, 3));
            var flattened = {};
            flattened["id"] = kmapid + "_relatedSubject_" + relatedSubject.category.id;
            // flattened["uid"] = kmapid + "_relatedSubject_" + relatedSubject.category.id;
            flattened["related_subjects_uid_s"] = "subjects-" + relatedSubject.category.id;
            flattened["block_child_type"] = "related_subjects";
            flattened["block_type"] = "child";
            flattened["related_subjects_title_s"] = relatedSubject.category.title;
            flattened["related_title_s"] = relatedSubject.category.title;
            flattened["related_subjects_category_id_i"] = relatedSubject.category.id;
            flattened["related_subjects_caption_s"] = relatedSubject.category.caption;
            flattened["related_subjects_label_s"] = relatedSubject.label;
            flattened["related_subjects_prefix_label_b"] = relatedSubject.prefix_label;
            flattened["related_subjects_parent_show_b"] = relatedSubject.show_parent;
            flattened["related_subjects_parent_title_s"] = relatedSubject.parent.title;
            flattened["related_subjects_root_show_b"] = relatedSubject.show_root;
            flattened["related_subjects_display_string_s"] = relatedSubject.display_string;
            if (relatedSubject.time_units.date)
                flattened["related_subjects_time_date_s"] = relatedSubject.time_units.date;
            flattened["related_subjects_time_is_range_b"] = (relatedSubject.time_units.is_range === true);
            if (relatedSubject.time_units.is_range)
                flattened["related_subjects_time_range_start_s"] = relatedSubject.time_units.start_date;
            if (relatedSubject.time_units.is_range)
                flattened["related_subjects_time_range_end_s"] = relatedSubject.time_units.end_date;
            // flattened["related_subjects_path_s"] = ancestorsToPath(relatedSubject.ancestors);
            flattened["related_subjects_numeric_value_i"] = relatedSubject.numeric_value;
            flattened["related_subjects_string_value_s"] = relatedSubject.string_value;
            finalRelatedSubjects.push(flattened);
        });
    } else {
        log.error ("No category_features! [ %s ] %j", kmapid, related_subject);
    }

    log.debug("returning %s", JSON.stringify(finalRelatedSubjects, undefined, 2));
    return finalRelatedSubjects;
}

