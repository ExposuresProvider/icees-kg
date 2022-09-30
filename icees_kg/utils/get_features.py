import numpy as np
import logging

LOGGER = logging.getLogger(__name__)


def get_feature_count_matrix(x1, x2, u_x1, u_x2, feature_1, feature_2):
    # TODO: This won't handle cases where there are invalid values in the data
    feat_count_mat = np.zeros((len(u_x2), len(u_x1)))

    errors = False
    for cx1, cx2 in zip(x1, x2):
        if (not np.isnan(cx1)) and (not np.isnan(cx2)):
            try:
                i_x1 = np.where(u_x1 == cx1)[0][0]
                i_x2 = np.where(u_x2 == cx2)[0][0]

                feat_count_mat[i_x2, i_x1] += 1  # x2 should be on the rows and x1 on the cols
            except IndexError as e:
                errors = True
                pass
    if errors:
        LOGGER.warning(f"Computed count matrix with errors for {feature_1} and {feature_2}")

    row_count = np.sum(feat_count_mat, axis=0)
    row_density = row_count / np.sum(row_count)

    # This is a little confusing because when you sum through the rows, you have something the length of the columns
    col_summary = [{"frequency": row_count[ind], "percentage": row_density[ind]} for ind in range(row_count.size)],  # cols will refer to feature_a (x1)

    col_count = np.sum(feat_count_mat, axis=1)
    col_density = col_count / np.sum(col_count)

    # This is a little confusing because when you sum through the columns, you have something the length of the rows
    row_summary = [{"frequency": col_count[ind], "percentage": col_density[ind]} for ind in range(col_count.size)],  # rows will refer to feature_b (x2)

    return feat_count_mat, row_summary, col_summary


def get_feature_info_from_column_info(column_info):
    """Get feature qualifiers from column info."""
    if column_info['is_integer']:
        feature_qualifiers = [
            {"operator": "=", "value": i + column_info['min']}
            for i in range(column_info['range'])
        ]
    else:  # enum
        feature_qualifiers = [
            {"operator": "=", "value": k}
            for k in column_info['enum']
        ]

    return {
        'feature_name': column_info['name'],
        'feature_qualifiers': feature_qualifiers
    }


def get_feature_matrix_json(count_mat):
    """Get feature matrix from count matrix."""
    row_counts = np.sum(count_mat, axis=1)
    total = np.sum(row_counts)
    col_counts = np.sum(count_mat, axis=0)

    matrix = []
    for i in range(count_mat.shape[0]):
        row = []
        for j in range(count_mat.shape[1]):
            v = count_mat[i, j]
            row_percentage = v / row_counts[i]
            if np.isnan(row_percentage):
                row_percentage = None
            col_percentage = v / col_counts[j]
            if np.isnan(col_percentage):
                col_percentage = None
            row.append({
                "frequency": v,
                "row_percentage": row_percentage,
                "col_percentage": col_percentage,
                "total_percentage": v / total,
            })
        matrix.append(row)

    return matrix
