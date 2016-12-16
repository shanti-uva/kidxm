/**
 * Created by ys2n on 12/7/16.
 */
const debug = require('debug')('flattenRelatedSubjects');
var debugRelatedSubjects = false;
var _ = require('underscore');

exports.flattenRelatedSubjects = function (kmapid,related_subject) {
    if (false) {
        debug("flattenRelatedSubjects=>");
        // log.info("%j",related_subject, {depth: 3, colors: 'true'});
    }

    var s = kmapid.split("-");
    var kid = s[1];
    var type = s[0];

    debug("kmapid = " + kmapid);
    debug("kid = " + kid);
    debug("flattenRelatedSubjects type = " + type);

    var relatedSubjects = related_subject.feature.category_features;

    var finalRelatedSubjects = [];

    var ancestorsToPath = function (ancestors) { return (_.map(ancestors, function (x) { return x.id; })).join('/'); };

    relatedSubjects.forEach(function (relatedSubject, y) {
        var flattened = {};
        flattened["id"] = kmapid + "_relatedSubject_" + relatedSubject.category.id;
        // flattened["uid"] = kmapid + "_relatedSubject_" + relatedSubject.category.id;
        flattened["related_subject_uid_s"] = "subjects-" + relatedSubject.category.id;
        flattened["block_child_type"] = "related_subject";
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
        flattened["related_subject_path_s"] = ancestorsToPath(relatedSubject.ancestors);
        finalRelatedSubjects.push(flattened);
    });
    return finalRelatedSubjects;
}

